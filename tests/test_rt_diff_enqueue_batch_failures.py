"""Differential attacks on enqueue_batch's failure boundary.

The bulk tier's whole-call contracts, compared across backends at the
moment the call fails:

* **Atomicity** - a statement-level failure mid-batch (an item whose id
  collides with an already-stored job; ``jobs_pkey`` has no ON CONFLICT
  arbiter in the batch INSERT) aborts the ENTIRE call on Postgres: one
  INSERT inside one transaction, so nothing is admitted. A mirror that
  stores the good prefix before the poisoned item raises certifies code
  that leaves phantom rows behind on PG.
* **Refusal observability** - a partitioned cap refusal must answer
  identically on both backends: same typed error, same refusal fields,
  and the same emitted ``max-pending-exceeded`` warning per refused
  actor. An operator's backpressure dashboards read the warning; a
  mirror that refuses silently lets a validated app ship with blank
  dashboards in production.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import UUID

import pytest
import structlog.testing

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.exceptions import BatchMaxPendingExceededError

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


def _batch_item(side: DiffSide, token: str, *, job_id: UUID | None = None) -> EnqueueArgs:
    """One uncapped batch item in the side's clock domain, token-registered."""
    args = EnqueueArgs(
        id=JobId(job_id) if job_id is not None else new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
    )
    side.register_job_id(token, args.id)
    return args


def _singleton_args(side: DiffSide, token: str, actor: str) -> EnqueueArgs:
    """One singleton-guarded batch item in the side's clock domain, token-registered."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
        metadata={"singleton": True},
    )
    side.register_job_id(token, args.id)
    return args


def _capped_args(side: DiffSide, token: str, actor: str, *, max_pending: int) -> EnqueueArgs:
    """One capped batch item in the side's clock domain, token-registered."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
        max_pending=max_pending,
    )
    side.register_job_id(token, args.id)
    return args


# ── Atomicity: a mid-batch statement failure aborts the whole call ──────


def _keyed_batch_item(
    side: DiffSide,
    token: str,
    actor: str,
    key: str,
) -> EnqueueArgs:
    """One idempotency-keyed batch item in the side's clock domain, token-registered."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
        idempotency_key=key,
    )
    side.register_job_id(token, args.id)
    return args


async def _mid_batch_cross_actor_idempotency_poison(side: DiffSide) -> None:
    """Two items of ONE batch call sharing an (idempotency_scope,
    idempotency_key) pair across actors, nothing pre-stored.

    PG's bulk tier is one unnest INSERT in one transaction: the second
    item's pair conflicts inside the statement (the ``ON CONFLICT``
    arbiter skips its row), the result assembly resolves it to the first
    item's just-inserted row, and the cross-actor refusal propagates out
    of the transaction, withdrawing the first item's row too. The mirror
    must refuse with the same typed error BEFORE its first insert, never
    store the good prefix and raise at the offending item's index.
    """
    good = _batch_item(side, "good")
    first = _keyed_batch_item(side, "first", "actor_a", "shared-key")
    second = _keyed_batch_item(side, "second", "actor_b", "shared-key")
    try:
        rows = await side.backend.enqueue_batch([good, first, second])
        side.record("batch", "admitted-all")
        side.record("returned", [side.token_of(r.id) for r in rows])
    except Exception as exc:  # Why: the differential records the typed outcome; the exception type IS the observable.
        side.record("batch", type(exc).__name__)
        side.record("mismatch_actor", getattr(exc, "actor", None))
        side.record("mismatch_existing_actor", getattr(exc, "existing_actor", None))
        holder_id = getattr(exc, "existing_job_id", None)
        side.record(
            "mismatch_existing_job_id",
            side.token_of(holder_id) if holder_id is not None else None,
        )
        side.record("mismatch_key", getattr(exc, "idempotency_key", None))
    stored_from_batch: list[str] = []
    for token, args in (("good", good), ("first", first), ("second", second)):
        if await side.backend.get(args.id) is not None:
            stored_from_batch.append(token)
    side.record("stored_from_batch", stored_from_batch)


async def test_diff_enqueue_batch_in_batch_cross_actor_idempotency_aborts_whole_call(
    pg_dsn: str,
) -> None:
    """Two items of one enqueue_batch call sharing an idempotency pair
    across actors must abort the WHOLE call on both backends: the
    mismatch raises with the first item's row withdrawn along with the
    batch, nothing stored, and the error names incoming actor, holder
    actor, and the holder row's id (the first item's) identically."""
    mem, pg = await run_differential(
        _mid_batch_cross_actor_idempotency_poison,
        pg_dsn=pg_dsn,
        actors=("test_actor", "actor_a", "actor_b"),
    )
    assert_mirror(
        "an in-batch cross-actor idempotency duplicate aborts the entire "
        "enqueue_batch call with the same typed attribution and no item "
        "from the batch stored, on either backend",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "IdempotencyKeyActorMismatchError"
    assert pg["records"]["mismatch_actor"] == "actor_b"
    assert pg["records"]["mismatch_existing_actor"] == "actor_a"
    assert pg["records"]["mismatch_existing_job_id"] == "first"
    assert pg["records"]["mismatch_key"] == "shared-key"
    assert pg["records"]["stored_from_batch"] == []
    assert pg["status_counts"] == {}


async def _mid_batch_duplicate_id_poison(side: DiffSide) -> None:
    stored = await side.enqueue("existing")
    good1 = _batch_item(side, "good1")
    poison = _batch_item(side, "poison", job_id=stored.id)
    good2 = _batch_item(side, "good2")
    try:
        rows = await side.backend.enqueue_batch([good1, poison, good2])
        side.record("batch", "admitted-all")
        side.record("returned", [side.token_of(r.id) for r in rows])
    except Exception as exc:  # Why: the differential records the typed outcome; the exception type IS the observable.
        side.record("batch", type(exc).__name__)
    stored_from_batch: list[str] = []
    for token, args in (("good1", good1), ("good2", good2)):
        if await side.backend.get(args.id) is not None:
            stored_from_batch.append(token)
    side.record("stored_from_batch", stored_from_batch)


async def test_diff_enqueue_batch_mid_batch_duplicate_id_aborts_whole_call(pg_dsn: str) -> None:
    """A batch item whose id duplicates a stored job must abort the WHOLE
    call on both backends: PG's bulk tier is one INSERT in one transaction
    (nothing admitted on a statement error); the mirror must leave the
    identical stored-row state - never the good prefix."""
    mem, pg = await run_differential(_mid_batch_duplicate_id_poison, pg_dsn=pg_dsn)
    assert_mirror(
        "a mid-batch constraint violation aborts the entire enqueue_batch "
        "call: no item from the batch is stored, on either backend",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "UniqueViolationError"
    assert pg["records"]["stored_from_batch"] == []
    assert pg["status_counts"] == {"pending": 1}


async def _mid_batch_singleton_collision_poison(side: DiffSide) -> None:
    """Two same-actor singleton items in ONE batch call, nothing pre-stored.

    PG's single unnest INSERT hits the jobs_singleton_uniq partial unique
    index and aborts the WHOLE statement - neither item is stored. The
    in-memory mirror's batch-level singleton preflight
    (``_check_batch_singletons`` in src/taskq/testing/_enqueue.py) refuses
    the whole call before storing its first row, so both backends leave
    the identical empty stored-row state.
    """
    item1 = _singleton_args(side, "item1", "test_actor")
    item2 = _singleton_args(side, "item2", "test_actor")
    try:
        rows = await side.backend.enqueue_batch([item1, item2])
        side.record("batch", "admitted-all")
        side.record("returned", [side.token_of(r.id) for r in rows])
    except Exception as exc:  # Why: the differential records the typed outcome; the exception type IS the observable.
        side.record("batch", type(exc).__name__)
    stored_from_batch: list[str] = []
    for token, args in (("item1", item1), ("item2", item2)):
        if await side.backend.get(args.id) is not None:
            stored_from_batch.append(token)
    side.record("stored_from_batch", stored_from_batch)


async def test_diff_enqueue_batch_mid_batch_singleton_collision_aborts_whole_call(
    pg_dsn: str,
) -> None:
    """A batch containing two singleton items for the same actor must abort
    the WHOLE call on both backends: PG's bulk tier is one INSERT in one
    transaction (jobs_singleton_uniq aborts it, nothing admitted); the
    mirror must leave the identical stored-row state - never the good
    prefix.
    """
    mem, pg = await run_differential(_mid_batch_singleton_collision_poison, pg_dsn=pg_dsn)
    assert_mirror(
        "a mid-batch singleton constraint violation aborts the entire "
        "enqueue_batch call: no item from the batch is stored, on either "
        "backend",
        mem,
        pg,
    )
    # Why the typed error: a singleton collision is a retryable admission
    # refusal, so the PG bulk tier converts the jobs_singleton_uniq
    # violation to the same typed SingletonCollisionError the
    # single-enqueue path raises - a caller must branch on it without
    # string-matching a raw driver error (the mirror's batch preflight
    # already refuses with the same typed error).
    assert pg["records"]["batch"] == "SingletonCollisionError"
    assert pg["records"]["stored_from_batch"] == []
    assert pg["status_counts"] == {}


# ── Atomic arm: a dedup hit survives a mid-stream failure ───────────────


async def _atomic_dedup_hit_then_stream_failure(side: DiffSide) -> None:
    """One atomic batch whose FIRST chunk is a dedup hit on a stored
    keyed row and whose SECOND chunk fails mid-stream (chunk_size=1 puts
    the failure strictly after the hit).

    PG's arm runs every chunk in one transaction: the failure rolls the
    TRANSACTION back, withdrawing only rows the transaction itself
    inserted, so the committed holder row a dedup hit returned stays
    stored. A compensating rollback that pops every returned row id
    withdraws the holder too.
    """
    holder = await side.enqueue("holder", idempotency_key="shared-key")
    hit = _keyed_batch_item(side, "hit", "test_actor", "shared-key")
    bid = new_uuid()

    def gen() -> Iterator[EnqueueArgs]:
        yield hit
        raise ValueError("generator exploded")

    try:
        rows = await side.backend.enqueue_batch_atomic(
            gen(),
            batch_id=bid,
            queue="default",
            batch_row=None,
            finalizer_args=None,
            chunk_size=1,
        )
        side.record("batch", "admitted-all")
        side.record("returned", [side.token_of(r.id) for r in rows])
    except Exception as exc:  # Why: the differential records the typed outcome; the exception type IS the observable.
        side.record("batch", type(exc).__name__)
    side.record("holder_survives", await side.backend.get(holder.id) is not None)


async def test_diff_enqueue_batch_atomic_dedup_hit_survives_mid_stream_failure(
    pg_dsn: str,
) -> None:
    """A failure after a dedup hit must leave the pre-existing holder row
    stored on both backends: the transaction (or its compensating
    rollback) withdraws only what the call itself inserted."""
    mem, pg = await run_differential(_atomic_dedup_hit_then_stream_failure, pg_dsn=pg_dsn)
    assert_mirror(
        "a failure after a dedup hit inside enqueue_batch_atomic leaves the "
        "pre-existing holder row stored and resolvable, on either backend",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "ValueError"
    assert pg["records"]["holder_survives"] is True
    assert pg["status_counts"] == {"pending": 1}


# ── Cap refusal: the error contract and the emitted log contract ────────


async def _cap_refusal_observability(side: DiffSide) -> None:
    await side.enqueue("existing-a1", actor="actor_a", max_pending=2)
    batch = [
        _capped_args(side, "a2", "actor_a", max_pending=2),
        _capped_args(side, "a3", "actor_a", max_pending=2),
        _capped_args(side, "b1", "actor_b", max_pending=2),
    ]
    with structlog.testing.capture_logs() as logs:
        try:
            rows = await side.backend.enqueue_batch(batch)
            side.record("batch", "admitted-all")
            side.record("returned", [side.token_of(r.id) for r in rows])
        except BatchMaxPendingExceededError as exc:
            side.record(
                "batch",
                {
                    "type": type(exc).__name__,
                    "refusals": sorted(
                        [r.actor, r.current_count, r.max_pending] for r in exc.refusals
                    ),
                    "refused_indices": dict(exc.refused_indices),
                    "admitted_count": exc.admitted_count,
                },
            )
        refusal_log: list[dict[str, Any]] = [
            {
                "actor": e["actor"],
                "log_level": e["log_level"],
                "current_count": e["current_count"],
                "max_pending": e["max_pending"],
            }
            for e in logs
            if e["event"] == "max-pending-exceeded"
        ]
    side.record("refusal_log", refusal_log)


async def test_diff_max_pending_batch_refusal_error_and_log_contract(pg_dsn: str) -> None:
    """A partitioned batch cap refusal answers with the same typed error,
    the same fields (per-refusal actor/current_count/max_pending, refused
    indices, admitted count), and the same emitted log contract - one
    ``max-pending-exceeded`` warning per refused actor carrying its
    facts - on both backends."""
    mem, pg = await run_differential(
        _cap_refusal_observability,
        pg_dsn=pg_dsn,
        actors=("test_actor", "actor_a", "actor_b"),
    )
    assert_mirror(
        "a partitioned batch cap refusal raises the same typed error with "
        "the same fields AND emits the same max-pending-exceeded warning "
        "per refused actor on both backends",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == {
        "type": "BatchMaxPendingExceededError",
        "refusals": [["actor_a", 1, 2]],
        "refused_indices": {"actor_a": [0, 1]},
        "admitted_count": 1,
    }
    assert pg["records"]["refusal_log"] == [
        {
            "actor": "actor_a",
            "log_level": "warning",
            "current_count": 1,
            "max_pending": 2,
        }
    ]
    assert pg["status_counts"] == {"pending": 2}
