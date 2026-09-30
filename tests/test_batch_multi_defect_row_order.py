"""Multi-defect batches raise the violation of the FIRST offending row, on
both backends.

A bulk statement (the unnest INSERT, the COPY) visits items in batch order
and aborts at the first row that violates an index it enforces; no later
row's defect can preempt it. A batch carrying SEVERAL defects therefore
raises a specific error decided by POSITION, not by the order a validator
happens to check families in: the in-memory mirror's whole-call preflights
run in a fixed order (jsonb, then per-tier family checks), so a
fixed-order mirror certifies a multi-defect batch raising a different
typed error than production raises - the worst kind of false confidence
the differential suite exists to catch.

Per-row precedence verified against live PG: ``jobs_pkey`` wins a row's
report over the composite idempotency index (a fast-tier re-run of the
exact same args raises the raw pkey violation, not the typed duplicate),
and a singleton collision on an earlier row wins over anything on a later
row (the statement never reaches the later row).

The pre-SQL families keep their statement order (jsonb build loop,
terminal-batch membership, cap partition - all before the bulk write);
what these pins hold is the ROW-ORDER precedence among the violations the
bulk statement itself enforces.
"""

from __future__ import annotations

from uuid import UUID

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration

_ACTORS = ("actor_a", "actor_b")


def _singleton_item(side: DiffSide, token: str, actor: str) -> EnqueueArgs:
    """One due singleton-flagged batch item (collides with any live
    singleton row of the same actor)."""
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


def _keyed_item(
    side: DiffSide, token: str, actor: str, key: str, *, job_id: UUID | None = None
) -> EnqueueArgs:
    """One due keyed batch item; ``job_id`` pins an explicit (colliding) id."""
    args = EnqueueArgs(
        id=job_id if job_id is not None else new_job_id(),
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


def _plain_item(
    side: DiffSide, token: str, actor: str, *, job_id: UUID | None = None
) -> EnqueueArgs:
    args = EnqueueArgs(
        id=job_id if job_id is not None else new_job_id(),
        actor=actor,
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
    )
    side.register_job_id(token, args.id)
    return args


async def _seed(side: DiffSide, key: str) -> tuple[None, object]:
    """Seed both defects' targets: a LIVE singleton row for actor_a and a
    stored pair for actor_b; return (None, the stored pair row's id)."""
    await side.enqueue("seed_singleton", actor="actor_a", metadata={"singleton": True})
    pair_row = await side.enqueue("seed_pair", actor="actor_b", idempotency_key=key)
    return None, pair_row.id


@pytest.mark.parametrize("fast", [False, True], ids=["unnest", "copy"])
async def test_earlier_row_singleton_collision_beats_a_later_rows_duplicate_pair(
    pg_dsn: str, fast: bool
) -> None:
    """[singleton-collision, duplicate-pair]: the singleton row comes
    first, so the typed singleton refusal raises - the duplicate pair is
    never reached."""
    key = "multi-defect-key-1"

    async def scenario(side: DiffSide) -> None:
        _, _pair_id = await _seed(side, key)
        items = [
            _singleton_item(side, "s1", "actor_a"),
            _keyed_item(side, "d1", "actor_b", key),
        ]
        try:
            if fast:
                await side.backend.enqueue_batch_fast(items)
            else:
                await side.backend.enqueue_batch(items)
            side.record("error", "admitted-all")
        except Exception as exc:
            side.record("error", type(exc).__name__)
        stored = [
            token
            for token, _ in (("s1", items[0]), ("d1", items[1]))
            if await side.backend.get(side._jobs_by_token[token]) is not None
        ]
        side.record("stored_from_batch", stored)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    assert_mirror(
        "a batch whose FIRST defect is a singleton collision raises "
        "SingletonCollisionError on both backends, however defective its "
        "later rows also are",
        mem,
        pg,
    )
    assert pg["records"]["error"] == "SingletonCollisionError"
    assert pg["records"]["stored_from_batch"] == []


@pytest.mark.parametrize("fast", [False, True], ids=["unnest", "copy"])
async def test_earlier_row_duplicate_pair_beats_a_later_rows_singleton_collision(
    pg_dsn: str, fast: bool
) -> None:
    """[duplicate-pair, singleton-collision] on the COPY tier: the
    duplicate is the row the statement aborts on, so the typed duplicate
    error raises and the singleton row is never reached. (The unnest tier
    dedupes the pair instead - its ON CONFLICT arbiter returns the stored
    row - and the singleton row then collides: the tiers legitimately
    disagree here, so each side is pinned against its own contract.)"""
    key = "multi-defect-key-2"

    async def scenario(side: DiffSide) -> None:
        _, _pair_id = await _seed(side, key)
        items = [
            _keyed_item(side, "d1", "actor_b", key),
            _singleton_item(side, "s1", "actor_a"),
        ]
        try:
            if fast:
                await side.backend.enqueue_batch_fast(items)
            else:
                await side.backend.enqueue_batch(items)
            side.record("error", "admitted-all")
        except Exception as exc:
            side.record("error", type(exc).__name__)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    if fast:
        assert_mirror(
            "the COPY tier aborts on the FIRST violating row - the duplicate "
            "pair - so the typed duplicate error raises on both backends",
            mem,
            pg,
        )
        assert pg["records"]["error"] == "DuplicateIdempotencyKeyError"
    else:
        assert pg["records"]["error"] == "SingletonCollisionError", (
            "the unnest tier dedupes the pair (no abort) and the singleton "
            f"row then collides; got {pg['records']['error']!r}"
        )
        assert mem["records"]["error"] == "SingletonCollisionError", (
            "the mirror's unnest tier must reach the same outcome: the pair "
            f"dedupes and the singleton refusal raises; got {mem['records']['error']!r}"
        )


@pytest.mark.parametrize("fast", [False, True], ids=["unnest", "copy"])
async def test_earlier_row_stored_id_beats_a_later_rows_duplicate_pair(
    pg_dsn: str, fast: bool
) -> None:
    """[stored-id, duplicate-pair]: the id collision is the row the write
    reaches first, so the raw jobs_pkey violation raises on both backends
    - the duplicate-pair classification never fires."""
    key = "multi-defect-key-3"

    async def scenario(side: DiffSide) -> None:
        _, pair_id = await _seed(side, key)
        # A second stored pair row so the duplicate at position 1 has a
        # stored holder even on the unnest tier (which dedupes instead of
        # aborting - the pkey row at position 0 still aborts first).
        await side.enqueue("seed_pair_2", actor="actor_b", idempotency_key=key + "-2")
        items = [
            _plain_item(side, "p1", "actor_c", job_id=pair_id),
            _keyed_item(side, "d1", "actor_b", key + "-2"),
        ]
        try:
            if fast:
                await side.backend.enqueue_batch_fast(items)
            else:
                await side.backend.enqueue_batch(items)
            side.record("error", "admitted-all")
        except Exception as exc:
            side.record("error", type(exc).__name__)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    assert_mirror(
        "a batch whose FIRST defect is a stored job id raises the raw "
        "jobs_pkey violation on both backends - the later row's duplicate "
        "pair is never reached",
        mem,
        pg,
    )
    assert pg["records"]["error"] == "UniqueViolationError"


@pytest.mark.parametrize("fast", [False, True], ids=["unnest", "copy"])
async def test_earlier_row_singleton_collision_beats_a_later_rows_stored_id(
    pg_dsn: str, fast: bool
) -> None:
    """[singleton-collision, stored-id]: the singleton row is the one the
    write reaches first, so the typed singleton refusal raises on both
    backends - the later row's pkey collision is never reached."""
    key = "multi-defect-key-4"

    async def scenario(side: DiffSide) -> None:
        _, pair_id = await _seed(side, key)
        items = [
            _singleton_item(side, "s1", "actor_a"),
            _plain_item(side, "p1", "actor_b", job_id=pair_id),
        ]
        try:
            if fast:
                await side.backend.enqueue_batch_fast(items)
            else:
                await side.backend.enqueue_batch(items)
            side.record("error", "admitted-all")
        except Exception as exc:
            side.record("error", type(exc).__name__)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    assert_mirror(
        "a batch whose FIRST defect is a singleton collision raises "
        "SingletonCollisionError on both backends even when a later row's "
        "id collides - the statement never reaches the later row",
        mem,
        pg,
    )
    assert pg["records"]["error"] == "SingletonCollisionError"


@pytest.mark.parametrize("fast", [False, True], ids=["unnest", "copy"])
async def test_exact_same_args_batch_rerun_dedupes_on_unnest_raises_pkey_on_copy(
    pg_dsn: str, fast: bool
) -> None:
    """Re-running the EXACT same args (same ids, same keys) through the
    unnest tier dedupes cleanly - the pair arbiter's ``DO NOTHING``
    suppresses each row, so ``jobs_pkey`` never fires. The COPY tier has
    no arbiter: the same re-run violates ``jobs_pkey`` on row 0 (the
    pkey wins the row's report over the composite index) and raises the
    raw violation. Both shapes are pinned per tier, on both backends."""
    key = "multi-defect-key-5"

    async def scenario(side: DiffSide) -> None:
        await side.enqueue("seed_pair", actor="actor_b", idempotency_key=key)
        seed_row = await side.backend.get(side._jobs_by_token["seed_pair"])
        assert seed_row is not None, "precondition: the seed row stored"
        items = [_keyed_item(side, "r1", "actor_b", key, job_id=seed_row.id)]
        try:
            if fast:
                await side.backend.enqueue_batch_fast(items)
            else:
                await side.backend.enqueue_batch(items)
            side.record("error", "admitted-all")
        except Exception as exc:
            side.record("error", type(exc).__name__)
        row = await side.backend.get(seed_row.id)
        side.record("holder_payload", row.payload if row is not None else None)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    if fast:
        assert pg["records"]["error"] == "UniqueViolationError", (
            "the COPY tier has no arbiter: the re-run violates jobs_pkey "
            f"(got {pg['records']['error']!r})"
        )
        assert mem["records"]["error"] == "UniqueViolationError", (
            "the mirror's COPY tier must raise the same raw pkey violation, "
            f"not the typed duplicate (got {mem['records']['error']!r})"
        )
    else:
        assert pg["records"]["error"] == "admitted-all", (
            f"the unnest arbiter dedupes the exact-same-args re-run; got {pg['records']['error']!r}"
        )
        assert mem["records"]["error"] == "admitted-all", (
            "the mirror's unnest tier must dedupe the exact-same-args "
            f"re-run, not raise the raw pkey violation (got {mem['records']['error']!r})"
        )
        assert mem["records"]["holder_payload"] == pg["records"]["holder_payload"], (
            "the dedup return must leave the stored row untouched on both backends"
        )
