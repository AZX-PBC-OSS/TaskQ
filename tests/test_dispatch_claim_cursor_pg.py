"""The claim cursor against real Postgres: the end-to-end cursor-behavior
pins, red before the cursor exists.

The unit pins (``tests/test_claim_cursor.py``) pin the store; these pin
the WIRE-UP - that the dispatch path actually feeds the cursor from
claimed rows, actually bounds the claim query with it, and that the
stranding trade the design accepts is BOUNDED on a real backend:

1. **the stranding bound** (the mutation target): a lower-id job inserted
   mid-flight - a skewed producer's UUIDv7 clock behind the worker's
   cursor - is claimable only after the jitter reset forgets the cursor;
   the pin proves the claim lands within ONE reset window.
2. **per-queue isolation**: another queue's high-water mark never strands
   a fresh queue's row - the memory is per queue, so a queue with no
   cursor of its own claims unbound.
3. **selection parity, cursor on vs off**: under monotone FIFO seeding -
   claim order == id order, the steady state - the cursor is a
   correctness no-op: the same rows claim in the same rounds. This is
   the pin that makes DEFAULT-ON defensible.
4. **the round-robin exemption**: an RR queue's round ignores the cursor
   entirely (the id-seek contradicts cohort fairness - a bounded RR
   round would strand whole cohorts for the window; documented, not
   shipped).
5. **the off switch**: ``claim_cursor_reset_seconds=0`` runs the naive
   shape - the A/B harness's naive arm and opting-out operators ride it.

The skewed lower-id rows every pin needs are minted by rewinding
uuid7's nanosecond clock (``uuid7(nanoseconds=...)``): a real UUIDv7
with a real producer's behind-the-worker timestamp, exactly the
"concurrent inserts with lower ids" shape the jitter reset exists to
bound.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta

import pytest
import uuid_utils

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_PAST = datetime(2025, 1, 1, tzinfo=UTC)

#: The jitter window the stranding pins run at: short enough that the
#: end-to-end wait is test-shaped, long enough that the cursor is live
#: across the pin's first rounds. The production default is 60s.
_SHORT_RESET = 1.0

#: Poll cadence while waiting out a reset window.
_POLL_S = 0.05


def _args(actor: str, queue: str = "default") -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_PAST,
    )


def _skewed_args(actor: str, queue: str, behind_seconds: float) -> EnqueueArgs:
    """A row whose UUIDv7 mints BEHIND the worker's cursor: the skewed-
    producer shape. uuid7's 48-bit ms clock rewound by *behind_seconds*."""
    from uuid import UUID as _UUID

    minted = uuid_utils.uuid7(  # noqa: TID251  # Why: the skewed-mint NEEDS the explicit-timestamp uuid7 form (a behind-the-worker clock); new_job_id() is the now-minting seam.
        nanoseconds=int(datetime.now(UTC).timestamp() * 1e9) - int(behind_seconds * 1e9)
    )
    return EnqueueArgs(
        id=_UUID(bytes=minted.bytes),
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_PAST,
    )


async def _ensure_actor(app: JobsApp, actor: str, queue: str = "default") -> None:
    schema = app.deps.settings.schema_name
    async with app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '  # noqa: S608  # Why: schema is fixture-derived (validated at settings load), not user input; every value is $-bound.
            "VALUES ($1, NULL, $2, '{}') ON CONFLICT (actor) DO UPDATE SET max_concurrent = NULL",
            actor,
            queue,
        )


# ── 1. the stranding bound (THE pin the mutation check breaks) ─────────


async def test_a_lower_id_job_is_claimed_within_one_reset_window(
    clean_jobs_app: JobsApp,
) -> None:
    """A skewed producer mints a UUIDv7 BELOW the worker's cursor. Under a
    live cursor that row is invisible to the claim query; the jitter reset
    is what makes it claimable. The pin: the row claims within one reset
    window (+ poll slack), proving the stranding bound is the window and
    nothing else."""
    app = clean_jobs_app
    app.deps.settings.claim_cursor_reset_seconds = _SHORT_RESET
    await _ensure_actor(app, "skew")

    # Seed + claim a first batch: the cursor lands on the newest id.
    for _ in range(3):
        await app.backend.enqueue(_args("skew"))
    rows = await app.backend.dispatch_batch(new_uuid(), ["default"], 10, _LEASE)
    assert len(rows) == 3, "the first (cursor-free) round claims the whole seed"

    skewed = _skewed_args("skew", "default", behind_seconds=10.0)
    await app.backend.enqueue(skewed)

    # Poll in rounds: empty while the cursor is live, then it claims.
    started = time.monotonic()
    deadline = _SHORT_RESET * 1.5 + 10.0  # the bound + poll slack
    claimed: list[JobRow] = []
    while time.monotonic() - started < deadline:
        claimed = await app.backend.dispatch_batch(new_uuid(), ["default"], 10, _LEASE)
        if claimed:
            break
        await asyncio.sleep(_POLL_S)
    elapsed = time.monotonic() - started
    assert claimed and any(r.id == skewed.id for r in claimed), (
        f"the skewed lower-id job must claim within one reset window "
        f"(waited {elapsed:.2f}s of a {deadline:.2f}s deadline): the "
        "stranding bound is the jitter window, and the window never came"
    )


# ── 2. per-queue isolation ─────────────────────────────────────────────


async def test_another_queues_cursor_never_strands_a_fresh_queue(
    clean_jobs_app: JobsApp,
) -> None:
    """Queue ``hot`` claimed (its cursor climbed high); queue ``fresh``
    receives a row whose id is BELOW that cursor. The memory is per queue:
    a round polling only ``fresh`` has no fresh-cursor to bound it and
    must claim immediately, never wait on hot's high-water mark."""
    app = clean_jobs_app
    app.deps.settings.claim_cursor_reset_seconds = 3600.0  # a window no pin outlives
    await _ensure_actor(app, "hot", queue="hot")
    await _ensure_actor(app, "fresh", queue="fresh")

    await app.backend.enqueue(_args("hot", "hot"))
    rows = await app.backend.dispatch_batch(new_uuid(), ["hot"], 10, _LEASE)
    assert len(rows) == 1

    below = _skewed_args("fresh", "fresh", behind_seconds=60.0)
    await app.backend.enqueue(below)
    rows = await app.backend.dispatch_batch(new_uuid(), ["fresh"], 10, _LEASE)
    assert [r.id for r in rows] == [below.id], (
        "a queue with its OWN (empty) claim history must claim unbound: "
        "hot's cursor is hot's memory, and a global cursor would strand "
        "fresh's below-cursor row for the whole window"
    )


# ── 3. selection parity: cursor on vs off under monotone FIFO ─────────


async def test_under_monotone_fifo_the_cursor_is_a_selection_noop(
    clean_jobs_app: JobsApp,
) -> None:
    """The steady state: monotone-id FIFO seeds - claim order == id order -
    so the cursor must select the SAME rows the naive shape would: all
    five rows claim in one round with the cursor live."""
    app = clean_jobs_app
    app.deps.settings.claim_cursor_reset_seconds = 3600.0
    await _ensure_actor(app, "fifo")

    ids = []
    for _ in range(5):
        args = _args("fifo")
        ids.append(args.id)
        await app.backend.enqueue(args)
    rows = await app.backend.dispatch_batch(new_uuid(), ["default"], 10, _LEASE)
    assert {r.id for r in rows} == set(ids), (
        "under monotone FIFO the cursor must be selection-inert: all 5 "
        "monotone rows claim in one round with the cursor live - this "
        "no-op property is what makes the cursor DEFAULT-ON defensible"
    )


# ── 4. the round-robin exemption ───────────────────────────────────────


async def test_round_robin_rounds_ignore_the_cursor(
    clean_jobs_app: JobsApp,
) -> None:
    """An RR queue's cohort fairness is id-order-blind by design; the
    cursor's id-bound would strand whole cohorts for a jitter window (the
    cohort starvation the mode exists to prevent). RR rounds are EXEMPT:
    a below-cursor row still claims."""
    app = clean_jobs_app
    app.deps.settings.claim_cursor_reset_seconds = 3600.0
    await _ensure_actor(app, "rr")

    await app.backend.enqueue(_args("rr"))
    rows = await app.backend.dispatch_batch(new_uuid(), ["default"], 10, _LEASE)
    assert len(rows) == 1

    below = _skewed_args("rr", "default", behind_seconds=60.0)
    await app.backend.enqueue(below)

    # Flip the mode the way a queue-ops writer does: the table row, then
    # the seam invalidation (a writer that skipped it would serve the
    # mode cache's 5s-stale strict mode - the stale-mode shape
    # QueueModeCache's docstring documents, not the RR exemption).
    schema = app.deps.settings.schema_name
    async with app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '  # noqa: S608  # Why: schema is fixture-derived (validated at settings load), not user input; every value is $-bound.
            "ON CONFLICT (name) DO UPDATE SET mode = $2",
            "default",
            "round_robin",
        )
    from taskq.backend._dispatch import invalidate_queue_mode_caches

    invalidate_queue_mode_caches()
    rows = await app.backend.dispatch_batch(new_uuid(), ["default"], 10, _LEASE)
    assert [r.id for r in rows] == [below.id], (
        "an RR round must ignore the claim cursor: the id-bound would "
        "strand whole fairness cohorts for a jitter window"
    )


# ── 5. the off switch ──────────────────────────────────────────────────


async def test_reset_seconds_zero_runs_the_naive_shape(
    clean_jobs_app: JobsApp,
) -> None:
    """``claim_cursor_reset_seconds=0``: the cursor is inert end-to-end -
    a below-cursor row claims IMMEDIATELY (no jitter wait), which is the
    naive shape and the A/B harness's control arm."""
    app = clean_jobs_app
    app.deps.settings.claim_cursor_reset_seconds = 0.0
    await _ensure_actor(app, "off")

    await app.backend.enqueue(_args("off"))
    rows = await app.backend.dispatch_batch(new_uuid(), ["default"], 10, _LEASE)
    assert len(rows) == 1

    below = _skewed_args("off", "default", behind_seconds=60.0)
    await app.backend.enqueue(below)
    rows = await app.backend.dispatch_batch(new_uuid(), ["default"], 10, _LEASE)
    assert [r.id for r in rows] == [below.id], (
        "with the cursor disabled a below-cursor row must claim "
        "immediately: the off switch is the naive shape, the stranding "
        "trade vanishes"
    )
