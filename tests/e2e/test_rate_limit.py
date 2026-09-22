"""Rate-limit e2e - the Dragonfly-backed token bucket paces cross-container dispatch.

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
  ``retry_after = (1 - tokens) / refill`` from the Lua script - the 0.2s
  cadence that paces every burst below.
- Re-dispatch of denied jobs is quantized by the leader's
  ``_scheduled_wake_loop``, which promotes ``scheduled`` → ``pending`` on a
  hardcoded 1.0s tick (``worker/leader.py``) and then NOTIFYs the wake
  channel. Observed spreads are therefore the 0.2s/token bucket arithmetic
  rounded UP to 1s tick boundaries (typically ~1.5-2.5s for the 12-job
  burst) - always LARGER than the naive 1.4s theory, so the thresholds
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
from .actors import DeliverWebhookPayload, deliver_webhook, persistence_probe

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
    between tests, so cross-test bucket survival is unobservable by design -
    this test drains the persistence bucket with 5 jobs (``run_id + "-drain"``),
    waits for them to complete, then enqueues 6 more (``run_id``) and counts
    how many carried the rate-limit denial marker.

    The proof runs on the DEDICATED slow-refill bucket
    (``e2e_persistence_bucket``, capacity 5, refill 0.25/s - see
    ``persistence_probe`` in actors.py), and the refill rate is the point.
    The previous design ran this choreography against the delivery bucket
    (capacity 5, refill 5/s): refilling at 5 tokens/s, the uncontrolled
    handoff between "drain batch observed complete" and "worker dispatches
    the measured batch" refills one token per 0.2 s, so the denial count was
    a continuous function of handoff latency - 0.5 s of handoff denies ~3 of
    6, and 1.0 s refills the "drained" bucket back to capacity, denying 0-1
    (CI red: ``0/6 measured jobs were rate-limit-denied`` with a limiter that
    was perfectly healthy). No fixed threshold on that bucket can separate
    the persisted branch from the lost branch, because the measured batch's
    token supply is set by the handoff, not by the bucket's persisted state.

    At 0.25 tokens/s the handoff is priced out: the drain drives the bucket
    to ~0 tokens (the 5 drain acquires spread tens of ms, refilling
    hundredths of a token), and any handoff under ~12 s refills fewer than 3
    tokens, so at least 4 of the 6 measured jobs are denied on their first
    dispatch - deterministically. The NOTIFY-driven handoff observed locally
    is ~0.5 s; the worker's poll-only fallback cadence is 1.0 s, an order of
    magnitude inside the headroom. A LOST bucket (fresh capacity 5) admits 5
    of 6 immediately and denies at most 1, so the >= 4 threshold clears that
    ceiling fourfold.

    Two guards, both asserted on the MEASURED batch.
    (1) At least 4 of the 6 measured jobs must carry the rate-limit denial
    marker: the reservation-denial handler is the only writer of
    ``metadata.awaiting = "rate_limit:<bucket>"`` (sticky through later
    success), so it proves denial-by-bucket specifically - not actor
    ``Snooze`` and not actor-not-found release - with no clocks involved.
    (2) The measured-spread threshold is 0.5s - corroborating evidence
    only: denied jobs are re-dispatched on the leader's 1.0s wake tick at
    the ~4s refill cadence, so the persisted branch spreads far wider than
    an un-throttled baseline; guard (1) does the real discriminating.
    """
    drain_id = f"{run_id}-drain"
    drain_handles = [
        await e2e_client.enqueue(
            persistence_probe,
            DeliverWebhookPayload(run_id=drain_id, endpoint_id=f"drain-{i}"),
        )
        for i in range(_DRAIN_SIZE)
    ]

    # Wait for the drain batch to complete so the bucket is depleted before
    # the measured batch is enqueued. The slow refill (0.25 tokens/s) keeps
    # it depleted across the uncontrolled handoff to the measured dispatch -
    # see the class docstring above for why the 5/s delivery bucket cannot.
    await wait_all(drain_handles, timeout=90)

    measured_handles = [
        await e2e_client.enqueue(
            persistence_probe,
            DeliverWebhookPayload(run_id=run_id, endpoint_id=f"measured-{i}"),
        )
        for i in range(_FOLLOWUP_SIZE)
    ]

    # Denied jobs return at the ~4s refill cadence, quantized up by the
    # leader's 1.0s wake tick: ~6 re-dispatch rounds ≈ 30s tail. 300s bounds
    # that with order-of-magnitude headroom (the module timeout is 900s).
    await wait_all(measured_handles, timeout=300)

    # Exactly-once delivery of the drain batch (orthogonal to the denial
    # guard below).
    drain_rows = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, drain_id, kind="delivered"
    )
    assert len(drain_rows) == _DRAIN_SIZE

    # Persistence proof (F2), asserted on the MEASURED batch: at least 4
    # of the 6 measured jobs must carry the rate-limit denial marker. The
    # reservation-denial handler (_handlers.py) is the ONLY writer of
    # metadata.awaiting = "<class>:<bucket_name>" - sticky through
    # later success (mark_succeeded never touches metadata) - so it proves
    # denial-by-bucket specifically, not actor Snooze, not actor-not-found
    # release, and (unlike the max_attempts bump, which it accompanies)
    # names the exact bucket. The persistence bucket refills at 0.25
    # tokens/s: the drain leaves it at ~0 tokens, and any drain-to-dispatch
    # handoff under ~12 s refills fewer than 3 tokens, so at least 4 of the
    # 6 measured jobs are denied on first dispatch - a property of the
    # bucket's PERSISTED state, not of the handoff's duration. A LOST
    # bucket (fresh capacity-5) admits 5 of 6 immediately, denying <= 1;
    # the >= 4 threshold clears that ceiling fourfold.
    measured_job_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in measured_handles]
    )
    assert len(measured_job_rows) == _FOLLOWUP_SIZE
    configured = persistence_probe.retry.max_attempts
    # Bucket name derived from the actor's declaration - the awaiting
    # prefix is an internal taxonomy string (e.g. "rate_limit:") the test
    # must not couple to (a prefix rename must not silently vacate the guard).
    bucket = persistence_probe.rate_limits[0]
    per_job = {
        row["id"]: (row["max_attempts"], json.loads(row["metadata"]).get("awaiting"))
        for row in measured_job_rows
    }
    denied = {j: (a, w) for j, (a, w) in per_job.items() if w is not None and w.endswith(bucket)}
    assert len(denied) >= 4, (
        f"only {len(denied)}/{_FOLLOWUP_SIZE} measured jobs were "
        f"rate-limit-denied (persisted-drained-bucket ⇒ ≥4, lost bucket ⇒ ≤1); "
        f"per-job (max_attempts, awaiting) with configured={configured}: "
        f"{', '.join(f'{j}:{a}/{w}' for j, (a, w) in sorted(per_job.items()))} - "
        "drained bucket state did not survive in Dragonfly?"
    )

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="delivered")
    assert len(rows) == _FOLLOWUP_SIZE
    assert {row["job_id"] for row in rows} == {handle.job_id for handle in measured_handles}

    first_at = min(row["at"] for row in rows)
    last_at = max(row["at"] for row in rows)
    spread_seconds = (last_at - first_at).total_seconds()
    assert spread_seconds >= 0.5
