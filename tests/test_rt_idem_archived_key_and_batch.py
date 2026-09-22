# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""RED-TEAM pins: archived keys free at prune (S3) and batch exactly-once (S4).

Scenario 3 - an ARCHIVED job re-enqueued with the same idempotency key. The
product decision, from the docs (``docs/guides/jobs-clients.md``:
"A key within a scope still dedupes **until pruned**"): the composite unique
index lives on the LIVE ``jobs`` table only, the prune's archive-move DELETEs
the live row, so the archived singleton's key is INVISIBLE to the index and
the key is FREE again. That is the documented semantic, not a duplicate-side-
effect hole: the work archived with a truthful terminal version, a re-enqueue
is a NEW business request, and the alternative (deduping against the archive
forever) is the lifetime-key pin the scope column exists to remove. Pinned:

* a terminal-but-LIVE row still dedupes (the key is pinned until pruned),
* after the archive, the same key enqueues a NEW job with a NEW id, no error,
  exactly one live row, the archive row untouched,
* the InMemory twin agrees: its index must drop the archived pair too
  (a stale entry is a phantom ``DuplicateIdempotencyKeyError`` on the fast
  path - a bulk import Postgres accepts - and a cap-discount against a row
  that does not exist).

Scenario 4 - batch enqueue with a duplicate key INSIDE one batch, a batch
that fails mid-way, and the retry. Pinned on real Postgres and on the twin:

* a same-actor pair repeated inside one ``enqueue_batch`` dedupes to ONE job
  (both items get the same handle),
* a mid-batch ``IdempotencyKeyActorMismatchError`` admits NOTHING (the whole
  batch is withdrawn), and the resubmission after the fix admits each item
  exactly once - no residue from the failed attempt,
* a whole-batch retry with identical args (the shape the pool-retry wrapper's
  re-run executes after an ambiguous COMMIT) returns the same rows and writes
  nothing new.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey
from taskq.exceptions import (
    DuplicateIdempotencyKeyError,
    IdempotencyKeyActorMismatchError,
)
from taskq.testing.fixtures import _open_pg_backend
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.worker.leader import prune_terminal_jobs

pytestmark = pytest.mark.integration


async def _fresh_backend(pg_dsn: str) -> tuple[Any, Any, Any, str]:
    """A backend on a fresh schema: ``(stack, deps, backend, schema)``."""
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    return stack, deps, backend, schema


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


# ── Scenario 3: the archived key is free (the documented semantic) ───────


async def test_archived_job_key_is_free_terminal_live_row_still_dedupes(pg_dsn: str) -> None:
    """The decision pin: dedupe horizon ends at the prune.

    While the terminal row is LIVE its key still dedupes to the SAME id
    (the key is pinned to the dead job until it ages out). Once the prune
    archives the row (moved to ``jobs_archive``, deleted from ``jobs``) the
    same key enqueues a NEW job: a new id, was-not-existing semantics,
    exactly one live row, and the archive row untouched.
    """
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    key = IdempotencyKey(f"rt-arch-{new_base62()}")
    worker_id = new_uuid()
    try:
        args = make_enqueue_args(idempotency_key=str(key))
        row1 = await backend.enqueue(args)

        # Terminal but LIVE: the key still dedupes to the same id.
        again = make_enqueue_args(idempotency_key=str(key))
        row_dedup = await backend.enqueue(again)
        assert row_dedup.id == row1.id, (
            "CONTRACT: a terminal-but-live row's key still dedupes - the key "
            "is pinned to the dead job until pruned."
        )

        # Claim and terminate the job, then backdate finished_at past the
        # retention and run the prune's archive move.
        claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=30))
        assert [j.id for j in claimed] == [row1.id], "the job must claim to terminalize"
        ok = await backend.mark_succeeded(
            row1.id,
            worker_id,
            attempt=claimed[0].attempt,
            claim_epoch=claimed[0].claim_epoch,
        )
        assert ok, "the terminal write must land"
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET finished_at = clock_timestamp() - interval '31 days' "
                "WHERE id = $1",
                row1.id,
            )
            result = await prune_terminal_jobs(
                conn,
                retention_per_status={"succeeded": timedelta(0)},
                archive_retention=timedelta(days=365),
                schema=schema,
            )
        assert result.archived == 1, f"the prune must archive the row; got {result}"

        # Archived: the key is FREE. Re-enqueue is a new business request.
        reenqueue = make_enqueue_args(idempotency_key=str(key))
        row2 = await backend.enqueue(reenqueue)
        assert row2.id != row1.id, (
            "CONTRACT (docs: 'dedupes until pruned'): an archived row's key no "
            "longer dedupes - the index is over live rows and the archive move "
            "deleted the row. Re-enqueue after the archive creates a NEW job, "
            "it must never resurrect or return the archived id."
        )
        async with deps.worker_pool.acquire() as conn:
            live: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                "WHERE idempotency_scope = $1 AND idempotency_key = $2",
                "",
                str(key),
            )
            archived: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1',
                row1.id,
            )
        assert live == 1, f"exactly one LIVE row may hold the key; got {live}"
        assert archived == 1, "the archive row must stand untouched"
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


# ── Scenario 4: batch atomicity and the exactly-once retry ───────────────


def _two_items_same_key(actor: str, scope: str, key: str) -> list[EnqueueArgs]:
    return [
        make_enqueue_args(actor=actor, idempotency_key=key, idempotency_scope=scope)
        for _ in range(2)
    ]


async def test_in_batch_duplicate_key_dedupes_to_one_job(pg_dsn: str) -> None:
    """A same-actor (scope, key) pair repeated INSIDE one enqueue_batch
    dedupes: both items resolve to the same job, one row exists."""
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    scope = f"rt-bscope-{new_base62()}"
    key = f"rt-bkey-{new_base62()}"
    try:
        items = _two_items_same_key("test_actor", scope, key)
        rows = await backend.enqueue_batch(items)
        assert len(rows) == 2
        assert rows[0].id == rows[1].id, (
            f"CONTRACT: an in-batch same-actor duplicate dedupes to ONE job; "
            f"got {rows[0].id} and {rows[1].id}."
        )
        async with deps.worker_pool.acquire() as conn:
            val: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                "WHERE idempotency_scope = $1 AND idempotency_key = $2",
                scope,
                key,
            )
        assert val == 1, f"exactly one row may exist for the pair; got {val}"
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


async def test_mid_batch_failure_admits_nothing_and_resubmission_is_exactly_once(
    pg_dsn: str,
) -> None:
    """A batch whose LATER item is a cross-actor duplicate fails mid-way:
    nothing from the batch is admitted (atomic), and the resubmission with
    the collision fixed admits each item exactly once - no residue from the
    failed attempt duplicates anything."""
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    scope = f"rt-bscope-{new_base62()}"
    key = f"rt-bkey-{new_base62()}"
    try:
        good_a = make_enqueue_args(actor="test_actor", idempotency_key=key, idempotency_scope=scope)
        bad = make_enqueue_args(
            actor="test_actor_other", idempotency_key=key, idempotency_scope=scope
        )
        good_b = make_enqueue_args(
            actor="test_actor", idempotency_key=f"{key}-2", idempotency_scope=scope
        )
        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend.enqueue_batch([good_a, bad, good_b])

        async with deps.worker_pool.acquire() as conn:
            val: int = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
        assert val == 0, (
            f"CONTRACT: a mid-batch idempotency refusal admits NOTHING (the "
            f"whole batch is withdrawn); found {val} rows from the failed batch."
        )

        # The resubmission with the collision fixed (the mismatched item
        # gets its own key): each item lands exactly once.
        fixed_bad = make_enqueue_args(
            actor="test_actor_other",
            idempotency_key=f"{key}-other",
            idempotency_scope=scope,
        )
        rows = await backend.enqueue_batch([good_a, fixed_bad, good_b])
        assert [r.id for r in rows] == [good_a.id, fixed_bad.id, good_b.id]
        async with deps.worker_pool.acquire() as conn:
            val = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
        assert val == 3, f"the resubmission admits exactly three rows; got {val}"
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


async def test_whole_batch_retry_with_identical_args_dedupes(pg_dsn: str) -> None:
    """A whole-batch retry with identical args (the shape the pool-retry
    wrapper's re-run executes after an ambiguous COMMIT) returns the same
    rows and writes nothing new."""
    stack, deps, backend, schema = await _fresh_backend(pg_dsn)
    scope = f"rt-bscope-{new_base62()}"
    try:
        items = [
            make_enqueue_args(
                actor="test_actor",
                idempotency_key=f"rt-rk-{new_base62()}-{i}",
                idempotency_scope=scope,
            )
            for i in range(3)
        ]
        rows1 = await backend.enqueue_batch(items)
        rows2 = await backend.enqueue_batch(items)

        assert [r.id for r in rows2] == [r.id for r in rows1], (
            "CONTRACT: the identical batch re-run dedupes to the same rows"
        )
        async with deps.worker_pool.acquire() as conn:
            val: int = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
        assert val == 3, f"the identical re-run must write nothing new; got {val} rows"
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


async def test_twin_in_batch_duplicate_and_archived_key_fast_path() -> None:
    """Twin differential, scenarios 3 and 4.

    The mirror must agree with Postgres on the in-batch duplicate (dedupe to
    one job) AND on the archived key (free after the archive: the fast path
    accepts it and the cap preflight does not discount against it). The stale
    index entry is the red this pin exists to hold: a phantom
    ``DuplicateIdempotencyKeyError`` for a key Postgres has already freed,
    and an over-admission past a capped actor's ``max_pending``.
    """
    from taskq.testing._runner import register_actor_config
    from taskq.testing.clock import FakeClock

    start = datetime(2026, 1, 1, tzinfo=UTC)
    past = start - timedelta(seconds=1)
    backend = InMemoryBackend(clock=FakeClock(start=start))
    register_actor_config(backend, actor="test_actor", max_pending=8)
    worker_id = new_uuid()

    # In-batch duplicate: both items resolve to one job.
    dup_items = [
        make_enqueue_args(idempotency_key="rt-twin-dup", scheduled_at=past) for _ in range(2)
    ]
    rows = await backend.enqueue_batch(dup_items)
    assert rows[0].id == rows[1].id, (
        f"twin: an in-batch same-actor duplicate dedupes to one job; got "
        f"{rows[0].id} and {rows[1].id}"
    )

    # Archive a keyed job, then reuse its key.
    keyed = await backend.enqueue(
        make_enqueue_args(idempotency_key="rt-twin-arch", scheduled_at=past)
    )
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=30))
    assert keyed.id in [j.id for j in claimed]
    stored = backend._jobs[keyed.id]  # pyright: ignore[reportAttributeAccessIssue]
    ok = await backend.mark_succeeded(
        keyed.id, worker_id, attempt=stored.attempt, claim_epoch=stored.claim_epoch
    )
    assert ok
    archived_row = backend._jobs[keyed.id]  # pyright: ignore[reportAttributeAccessIssue]
    import dataclasses

    backend._jobs[keyed.id] = dataclasses.replace(  # pyright: ignore[reportAttributeAccessIssue]
        archived_row, finished_at=start - timedelta(days=31)
    )
    result = backend.archive_terminal_jobs(timedelta(days=30), timedelta(days=365))
    assert result.archived == 1, "the twin archive must move the terminal row"

    # The fast path accepts the freed key exactly like Postgres' COPY,
    # checked BEFORE anything re-registers the pair.
    count = await backend.enqueue_batch_fast(
        [make_enqueue_args(idempotency_key="rt-twin-arch", scheduled_at=past)]
    )
    assert count == 1, (
        "twin: the archived key is free - the fast path must accept a bulk "
        "import reusing it, Postgres' COPY does (the index is over live rows). "
        "A phantom DuplicateIdempotencyKeyError here is the stale-index red."
    )
    # ...and a second fast item on the now-live pair refuses (it IS stored).
    with pytest.raises(DuplicateIdempotencyKeyError):
        await backend.enqueue_batch_fast(
            [make_enqueue_args(idempotency_key="rt-twin-arch", scheduled_at=past)]
        )
    # ...and the fresh key enqueued above did not linger either.
    count = await backend.enqueue_batch_fast(
        [make_enqueue_args(idempotency_key="rt-twin-fresh", scheduled_at=past)]
    )
    assert count == 1

    # The cap preflight does not discount against an archived pair: a capped
    # actor's live pending count alone decides admission.
    capped = InMemoryBackend(clock=FakeClock(start=start))
    register_actor_config(capped, actor="test_actor", max_pending=1)
    capped_terminal = await capped.enqueue(
        make_enqueue_args(idempotency_key="rt-twin-cap-dead", scheduled_at=past)
    )
    claimed_c = await capped.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=30))
    stored_c = capped._jobs[capped_terminal.id]  # pyright: ignore[reportAttributeAccessIssue]
    ok = await capped.mark_succeeded(
        capped_terminal.id, worker_id, attempt=stored_c.attempt, claim_epoch=stored_c.claim_epoch
    )
    assert ok and claimed_c
    archived_c = capped._jobs[capped_terminal.id]  # pyright: ignore[reportAttributeAccessIssue]
    capped._jobs[capped_terminal.id] = dataclasses.replace(  # pyright: ignore[reportAttributeAccessIssue]
        archived_c, finished_at=start - timedelta(days=31)
    )
    capped.archive_terminal_jobs(timedelta(days=30), timedelta(days=365))
    # The actor is now at zero live rows; fill the cap with one pending job.
    live_pending = await capped.enqueue(
        make_enqueue_args(idempotency_key="rt-twin-cap-live", scheduled_at=past)
    )
    assert live_pending.status == "pending"
    # A NEW item with the archived pair is NOT a dedup hit (the row is
    # gone), it counts toward the cap, and the batch refuses it. The cap
    # rides the ARGS (batch_cap_groups groups carried caps only, the shape
    # the client resolves from actor config), so the item carries it.
    from taskq.exceptions import BatchMaxPendingExceededError

    with pytest.raises(BatchMaxPendingExceededError) as cap_refusal:
        await capped.enqueue_batch(
            [
                dataclasses.replace(
                    make_enqueue_args(idempotency_key="rt-twin-cap-dead", scheduled_at=past),
                    max_pending=1,
                )
            ]
        )
    assert cap_refusal.value.actor == "test_actor", (
        "twin: the archived pair must NOT discount the item from the cap "
        "preflight - the pair's row is gone, the item writes, so a "
        "max_pending=1 actor at capacity must refuse it. Over-admitting here "
        "is the stale-index cap hole."
    )
    assert len(capped._jobs) == 1, (  # pyright: ignore[reportAttributeAccessIssue]
        f"twin: no over-admission past max_pending=1; store holds {len(capped._jobs)} rows"
    )
