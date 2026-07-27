"""Rate-limit e2e — the Dragonfly-backed token bucket paces cross-container dispatch.

Design spec scenario row (docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
token bucket capacity 5, refill 5/s; burst 12 webhook jobs → last-vs-first
completion spread ≥ 1.0s (theory ~1.4s; un-throttled would be <0.3s); all
succeed. Ground truth: ``e2e_effects.at``.

Pacing mechanics verified against the library, not guessed:

- ``deliver_webhook`` declares ``rate_limits=["e2e_webhook_delivery"]``
  (tests/e2e/actors.py); the bucket is ``TokenBucket(capacity=5,
  refill_per_second=5.0, backend="redis")`` registered at actors import time.
- The consumer acquires a token BEFORE the actor body
  (``worker/_consumer.py`` → ``RateLimitRegistry.acquire_for_actor``). On
  denial it routes to ``_handle_reservation_class_denied`` →
  ``backend.mark_snoozed``, which deliberately does NOT consume retry budget
  (``backend/_sql_templates.py`` ``mark_snoozed`` leaves ``j.attempt``
  unchanged), so paced jobs survive unlimited denials.
- Denied jobs return to ``scheduled`` at ``now() + retry_after`` with
  ``retry_after = (1 - tokens) / refill`` from the Lua script — the 0.2s
  cadence that paces every burst below.
- Re-dispatch of denied jobs is quantized by the leader's
  ``_scheduled_wake_loop``, which promotes ``scheduled`` → ``pending`` on a
  hardcoded 1.0s tick (``worker/leader.py``) and then NOTIFYs the wake
  channel. Observed spreads are therefore the 0.2s/token bucket arithmetic
  rounded UP to 1s tick boundaries (typically ~1.5-2.5s for the 12-job
  burst) — always LARGER than the naive 1.4s theory, so the thresholds
  below are conservative under both models.
- The e2e worker env does not override ``TASKQ_MAX_CONCURRENCY``, so the
  settings default of 8 applies: the capacity-5 burst round runs effectively
  in parallel, keeping the first-five spread tight and the paced tail the
  dominant signal.

Every test requests ``e2e_worker`` explicitly: the worker container fixture
is not autouse, so no worker (and no dispatch) exists unless a test pulls it
in.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from ._assertions import fetch_effects
from .actors import DeliverWebhookPayload, deliver_webhook

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_BURST_SIZE = 12
_DRAIN_SIZE = 5
_FOLLOWUP_SIZE = 6


async def test_token_bucket_throttles_burst(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """12 jobs through a capacity-5 / 5-per-second bucket are visibly paced.

    5 jobs run on the initial tokens; the remaining 7 are paced at the 0.2s
    refill cadence → last-vs-first effect spread ≈ 1.4s. ``handle.wait()``
    raises ``JobFailed`` on any non-success terminal state, so a clean
    ``gather`` is itself the all-12-succeeded assertion; the effects check
    then proves each job ran exactly once. The 1.0s threshold keeps ≥2x
    slack over an un-throttled baseline (<0.3s) while sitting well under
    the ~1.4s theory.
    """
    handles = [
        await e2e_client.enqueue(
            deliver_webhook,
            DeliverWebhookPayload(run_id=run_id, endpoint_id=f"ep-{i:02d}"),
        )
        for i in range(_BURST_SIZE)
    ]

    await asyncio.gather(*(handle.wait(timeout=90) for handle in handles))

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="delivered")
    assert len(rows) == _BURST_SIZE
    assert {row["job_id"] for row in rows} == {handle.job_id for handle in handles}

    first_at = min(row["at"] for row in rows)
    last_at = max(row["at"] for row in rows)
    spread_seconds = (last_at - first_at).total_seconds()
    assert spread_seconds >= 1.0


async def test_rate_limit_state_survives_in_dragonfly(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """The drained bucket state persists in Dragonfly across enqueue batches.

    The autouse ``clean_e2e_state`` reset FLUSHDBs the module's logical DB
    between tests, so cross-test bucket survival is unobservable by design —
    this test instead drains the bucket with 5 jobs (``run_id + "-drain"``)
    and immediately enqueues 6 more (``run_id``) without waiting in between.
    If the bucket state were lost between the batches, the 6 would face a
    full capacity-5 bucket and finish within ~0.3s. Because the drained
    state persists, they are paced purely by the 5/s refill: FIFO dispatch
    (``scheduled_at`` order) lets the 5 drain jobs claim the initial tokens,
    so the 6 measured jobs land at ~0.2s … ~1.2s (≈1.0s spread; worse-case
    interleavings only push the last measured effect later).

    Two guards make the persistence proof airtight (F2). (1) The 5 drain
    jobs' effect timestamps must cluster within 0.3s, proving they consumed
    the initial capacity-5 burst in parallel (~0.03s actor latency) rather
    than being partially snoozed — a snoozed drain would leave the measured
    jobs a partially-full bucket and a sub-threshold spread. (2) The
    measured-spread threshold is 0.8s: just below the ~1.0s theory, well
    above the ~0.3s bucket-lost baseline. The old 0.5s bar was only ~1.7x
    baseline and could pass on a partially-drained bucket.
    """
    drain_id = f"{run_id}-drain"
    drain_handles = [
        await e2e_client.enqueue(
            deliver_webhook,
            DeliverWebhookPayload(run_id=drain_id, endpoint_id=f"drain-{i}"),
        )
        for i in range(_DRAIN_SIZE)
    ]
    measured_handles = [
        await e2e_client.enqueue(
            deliver_webhook,
            DeliverWebhookPayload(run_id=run_id, endpoint_id=f"measured-{i}"),
        )
        for i in range(_FOLLOWUP_SIZE)
    ]

    await asyncio.gather(
        *(handle.wait(timeout=90) for handle in [*drain_handles, *measured_handles])
    )

    # Burst-consumption proof (F2): the drain jobs must have run in parallel
    # on the initial capacity-5 tokens, so their effect timestamps cluster
    # tightly. A wider cluster means some were snoozed, which would leave
    # the measured jobs a partially-full bucket and invalidate the spread
    # assertion below.
    drain_rows = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, drain_id, kind="delivered"
    )
    assert len(drain_rows) == _DRAIN_SIZE
    drain_spread_seconds = (
        max(row["at"] for row in drain_rows) - min(row["at"] for row in drain_rows)
    ).total_seconds()
    assert drain_spread_seconds <= 0.3, (
        f"drain jobs spread over {drain_spread_seconds:.2f}s — "
        "initial burst tokens not consumed by the drain batch?"
    )

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="delivered")
    assert len(rows) == _FOLLOWUP_SIZE
    assert {row["job_id"] for row in rows} == {handle.job_id for handle in measured_handles}

    first_at = min(row["at"] for row in rows)
    last_at = max(row["at"] for row in rows)
    spread_seconds = (last_at - first_at).total_seconds()
    assert spread_seconds >= 0.8
