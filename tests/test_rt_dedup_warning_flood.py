"""Red-team: batch-scale dedup WARNINGs are unbounded per-item emissions.

The contract under attack: an ``enqueue_batch()`` whose items all dedup
emits one ``enqueue_deduplicated`` line per item — WARNING for every item
that lands on a terminal target — with no aggregation or window-gating.
A re-submitted 1000-item batch against 500 terminal and 500 live targets
emits 500 WARNING lines from a single call. That is a flood, not a
signal: operators answer a per-item WARNING flood by muting the channel,
destroying the signal the WARNING exists to raise.

The intended contract is the codebase's own answer to this exact flood
shape — the window-gated dependency-failure WARNING in
``worker/_consumer.py`` (``_DEPENDENCY_FAILURE_LOG_WINDOW_S``, one
warning per window per error type): the WARNING emissions for a dedup
flood are aggregated or window-gated to a small bounded count, while the
per-hit INFO/status observability that ``TestBatchDedupIsObservable``
(tests/test_postgres_enqueue_batch_collision.py) pins for small batches
survives untouched.
"""

import pytest
import structlog

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration

#: The batch size at which a per-item WARNING stops being a signal and
#: becomes channel noise an operator mutes — the claim's own scale.
_FLOOD_N = 1000

#: How many seeded targets are driven terminal before the attack batch:
#: the WARNING-arm share of the flood. The rest stay pending, so the
#: same batch also exercises the INFO per-hit arm at full scale.
_TERMINAL_N = 500

#: The bound the intended contract demands. The house precedent
#: (one WARNING per window per error type) lands at 1; a per-batch
#: summary line lands at 1; 5 leaves headroom for either shape while
#: staying a small constant — and stays falsified by any per-item
#: emission at the seeded scale.
_MAX_BATCH_DEDUP_WARNINGS = 5


def _make_args(key: str, *, payload_index: int) -> EnqueueArgs:
    """The repeated shape of every item in both scenarios below."""
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"i": payload_index},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=None,
        idempotency_key=IdempotencyKey(key),
    )


async def test_batch_dedup_warning_flood_is_bounded(clean_jobs_app: JobsApp) -> None:
    """A fully-deduped large batch emits a small bounded number of WARNINGs.

    Seeded: 1000 stored jobs, the first 500 driven terminal (``failed``),
    the rest live (``pending``); the attack batch re-enqueues 1000 items
    carrying the same idempotency keys, so every item dedups — 500 onto
    terminal targets (the WARNING arm) and 500 onto live ones (the INFO
    arm). The WARNING emissions for that flood are aggregated or
    window-gated to a small bounded count.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema: str = deps.settings.schema_name

    seed_args = [_make_args(f"flood-dedup-{i:04d}", payload_index=i) for i in range(_FLOOD_N)]
    seed_rows = await backend.enqueue_batch(seed_args)
    assert len(seed_rows) == _FLOOD_N, "precondition: the seed batch must fully insert"

    terminal_ids = [row.id for row in seed_rows[:_TERMINAL_N]]
    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the JobsApp shim, as elsewhere in the suite.
        status_tag: str = await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema is the fixture's validated identifier; the ids are $-bound.
            "SET status = 'failed', finished_at = now() "
            "WHERE id = ANY($1::uuid[])",
            terminal_ids,
        )
    assert int(status_tag.rsplit(" ", 1)[-1]) == _TERMINAL_N, (
        f"precondition: exactly {_TERMINAL_N} targets must be driven terminal; got {status_tag!r}"
    )

    attack_args = [_make_args(f"flood-dedup-{i:04d}", payload_index=i) for i in range(_FLOOD_N)]
    with structlog.testing.capture_logs() as captured:
        attack_rows = await backend.enqueue_batch(attack_args)

    assert [row.id for row in attack_rows] == [row.id for row in seed_rows], (
        "precondition: every attack item must dedup onto its seeded row"
    )
    assert all(row.status == "failed" for row in attack_rows[:_TERMINAL_N]), (
        "precondition: the first half of the attack items dedup onto TERMINAL targets"
    )

    hits = [e for e in captured if e.get("event") == "enqueue_deduplicated"]
    warnings = [e for e in hits if e.get("log_level") == "warning"]
    infos = [e for e in hits if e.get("log_level") == "info"]

    assert len(warnings) <= _MAX_BATCH_DEDUP_WARNINGS, (
        f"a fully-deduped {_FLOOD_N}-item batch ({_TERMINAL_N} items onto TERMINAL "
        f"targets) emitted {len(warnings)} WARNING-level enqueue_deduplicated lines "
        f"(beside {len(infos)} info-level hits) — one WARNING per item, unaggregated "
        "and unwindow-gated. A per-item WARNING flood at batch scale is channel "
        "noise an operator mutes, destroying the signal it exists to raise; the "
        "dependency-failure WARNING in worker/_consumer.py is window-gated for "
        "exactly this flood shape, and the dedup WARNING must aggregate or "
        "window-gate at batch scale the same way — without removing the per-hit "
        "INFO/status observability pinned for small batches."
    )


async def test_small_batch_dedup_keeps_per_hit_info_status(clean_jobs_app: JobsApp) -> None:
    """A small all-dup batch keeps one INFO status line per hit.

    The bound the flood test demands is not bought by silencing the
    per-hit observability: every hit on a small batch still gets its own
    line at INFO, carrying the target's status — the contract
    ``TestBatchDedupIsObservable`` pins for the one-item shape, held here
    for a small multi-item batch.
    """
    backend = clean_jobs_app.backend

    keys = [f"small-dedup-{i}" for i in range(3)]
    seed_rows = await backend.enqueue_batch(
        [_make_args(key, payload_index=i) for i, key in enumerate(keys)]
    )
    assert len(seed_rows) == 3

    with structlog.testing.capture_logs() as captured:
        dedup_rows = await backend.enqueue_batch(
            [_make_args(key, payload_index=i) for i, key in enumerate(keys)]
        )

    assert [row.id for row in dedup_rows] == [row.id for row in seed_rows], (
        "precondition: every item must dedup onto its seeded row"
    )

    hits = [e for e in captured if e.get("event") == "enqueue_deduplicated"]
    assert len(hits) == 3, f"one line per hit on a small batch; got {len(hits)}: {hits!r}"
    assert all(e.get("log_level") == "info" for e in hits), (
        f"a live-target hit on a small batch stays INFO; got {hits!r}"
    )
    assert all(e.get("status") == "pending" for e in hits), (
        f"every hit carries the target's status; got {hits!r}"
    )
