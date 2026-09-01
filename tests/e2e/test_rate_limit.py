"""Rate-limit e2e — the Dragonfly-backed token bucket paces cross-container dispatch.

Scenario:
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
- Denied jobs return to ``scheduled`` at ``clock_timestamp() + retry_after`` with
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

import json
from typing import TYPE_CHECKING

import pytest

from ._assertions import fetch_effects, fetch_job_rows, wait_all
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

    await wait_all(handles, timeout=90)

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
    this test drains the bucket with 5 jobs (``run_id + "-drain"``), waits
    for them to complete, then enqueues 6 more (``run_id``). Waiting for
    the drain to finish guarantees the bucket is fully depleted (0 tokens
    remaining) before the measured batch is dispatched, making the denial
    count deterministic rather than dependent on the scheduling-dependent
    interleaving of drain and measured jobs in a concurrently-claimed
    first batch.

    The previous design enqueued both batches back-to-back without waiting,
    relying on FIFO claiming to let drain jobs win the initial tokens.
    That proved fragile: with a 5/s refill rate, 0.2 s of dispatch skew
    (plausible in CI from DI resolution, Redis round-trips, and scheduling
    overhead) refills one extra token, allowing an additional measured job
    to win and reducing the denial count below the threshold.

    Two guards make the persistence proof airtight (F2), both asserted on
    the MEASURED batch.
    (1) At least 2 of the 6 measured jobs must carry the rate-limit denial
    marker: the reservation-denial handler is the only writer of
    ``metadata.awaiting = "rate_limit:<bucket>"`` (sticky through later
    success), so it proves denial-by-bucket specifically — not actor
    ``Snooze`` and not actor-not-found release — with no clocks involved.
    After the drain completes, the bucket has 0 tokens and refills at 5/s
    (1 token per 0.2 s). The test process enqueues 6 measured jobs
    immediately after the drain's ``gather`` returns; the time between
    drain completion and the worker dispatching the measured batch is
    bounded by the producer's poll interval (well under 1 s), so at most
    ~5 tokens have refilled — but the first 5 measured jobs that acquire
    consume those refilled tokens, leaving the 6th (and possibly the 5th)
    denied. A persisted-drained bucket typically denies ≥ 4; a LOST bucket
    (fresh capacity-5) denies ≤ 1. The ≥ 2 threshold sits comfortably
    above the lost-bucket ceiling with generous headroom for refill
    trickle.
    (2) The measured-spread threshold is 0.5s — corroborating evidence
    only: under a lost bucket the lone denied job is re-promoted on the
    1.0s wake tick, so its phase-dependent spread ([0.2s, 1.2s]) can clear
    0.5s by luck; guard (1) does the real discriminating.
    """
    drain_id = f"{run_id}-drain"
    drain_handles = [
        await e2e_client.enqueue(
            deliver_webhook,
            DeliverWebhookPayload(run_id=drain_id, endpoint_id=f"drain-{i}"),
        )
        for i in range(_DRAIN_SIZE)
    ]

    # Wait for the drain batch to complete so the bucket is fully
    # depleted before the measured batch is enqueued. This makes the
    # denial count deterministic: the measured batch faces a guaranteed
    # 0-token bucket (refilling at 5/s) rather than competing with drain
    # jobs for the initial capacity-5 tokens in a scheduling-dependent
    # first batch.
    await wait_all(drain_handles, timeout=90)

    measured_handles = [
        await e2e_client.enqueue(
            deliver_webhook,
            DeliverWebhookPayload(run_id=run_id, endpoint_id=f"measured-{i}"),
        )
        for i in range(_FOLLOWUP_SIZE)
    ]

    await wait_all(measured_handles, timeout=90)

    # Exactly-once delivery of the drain batch (orthogonal to the denial
    # guard below).
    drain_rows = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, drain_id, kind="delivered"
    )
    assert len(drain_rows) == _DRAIN_SIZE

    # Persistence proof (F2), asserted on the MEASURED batch: at least 2
    # of the 6 measured jobs must carry the rate-limit denial marker. The
    # reservation-denial handler (_handlers.py) is the ONLY writer of
    # metadata.awaiting = "<class>:<bucket_name>" — sticky through
    # later success (mark_succeeded never touches metadata) — so it proves
    # denial-by-bucket specifically, not actor Snooze, not actor-not-found
    # release, and (unlike the max_attempts bump, which it accompanies)
    # names the exact bucket. After the drain completes the bucket has 0
    # tokens; the 6 measured jobs are enqueued immediately, so the worker
    # dispatches them into a depleted bucket. At most ~5 tokens refill in
    # the ~1s between drain completion and the first measured dispatch
    # (producer poll + wake tick), so 5 measured jobs may win refilled
    # tokens, but the 6th is denied — and typically ≥ 4 are denied because
    # the refill trickle is sub-second. A LOST bucket (fresh capacity-5)
    # denies ≤ 1; the ≥ 2 threshold clears that ceiling with headroom.
    measured_job_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in measured_handles]
    )
    assert len(measured_job_rows) == _FOLLOWUP_SIZE
    configured = deliver_webhook.retry.max_attempts
    # Bucket name derived from the actor's declaration — the awaiting
    # prefix is an internal taxonomy string (e.g. "rate_limit:") the test
    # must not couple to (a prefix rename must not silently vacate the guard).
    bucket = deliver_webhook.rate_limits[0]
    per_job = {
        row["id"]: (row["max_attempts"], json.loads(row["metadata"]).get("awaiting"))
        for row in measured_job_rows
    }
    denied = {j: (a, w) for j, (a, w) in per_job.items() if w is not None and w.endswith(bucket)}
    assert len(denied) >= 2, (
        f"only {len(denied)}/{_FOLLOWUP_SIZE} measured jobs were "
        f"rate-limit-denied (persisted-drained-bucket ⇒ ≥4, lost bucket ⇒ ≤1); "
        f"per-job (max_attempts, awaiting) with configured={configured}: "
        f"{', '.join(f'{j}:{a}/{w}' for j, (a, w) in sorted(per_job.items()))} — "
        "drained bucket state did not survive in Dragonfly?"
    )

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="delivered")
    assert len(rows) == _FOLLOWUP_SIZE
    assert {row["job_id"] for row in rows} == {handle.job_id for handle in measured_handles}

    first_at = min(row["at"] for row in rows)
    last_at = max(row["at"] for row in rows)
    spread_seconds = (last_at - first_at).total_seconds()
    assert spread_seconds >= 0.5
