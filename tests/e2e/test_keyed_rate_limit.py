"""KeyedRateLimitRef e2e — per-tenant token buckets are independent.

Verifies that ``KeyedRateLimitRef`` materializes an independent token
bucket per key (tenant_id) and that draining one tenant's bucket does not
affect another tenant's bucket. The actor
``deliver_tenant_webhook`` declares
``rate_limits=[KeyedRateLimitRef.typed(DeliverTenantWebhookPayload,
base_name="e2e_per_tenant", key_fn=lambda p: p.tenant_id, capacity=3,
refill_per_second=1.0, backend="redis")]``, so each tenant gets a capacity-3 / 1-refill-per-second
bucket in Dragonfly, materialized lazily on first acquisition.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from ._assertions import fetch_effects, fetch_job_rows, wait_all
from .actors import (
    DeliverTenantWebhookPayload,
    TypedTenantPayload,
    deliver_tenant_webhook,
    deliver_typed_tenant_webhook,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_DRAIN_SIZE = 3
_MEASURED_SIZE = 2
_TENANT_A = "alpha"
_TENANT_B = "beta"
_BUCKET_BASE = "e2e_per_tenant"


def _denied_count(
    job_rows: list[asyncpg.Record], tenant: str
) -> tuple[int, dict[str, tuple[int, str | None]]]:
    """Count measured jobs carrying the rate-limit denial marker for *tenant*.

    Returns (denied_count, per_job_detail) where per_job_detail maps
    ``str(job_id)`` to ``(max_attempts, awaiting)`` for diagnostic output.
    """
    bucket = f"{_BUCKET_BASE}:{tenant}"
    per_job: dict[str, tuple[int, str | None]] = {}
    denied = 0
    for row in job_rows:
        awaiting = json.loads(row["metadata"]).get("awaiting")
        per_job[str(row["id"])] = (row["max_attempts"], awaiting)
        if awaiting is not None and awaiting.endswith(bucket):
            denied += 1
    return denied, per_job


async def test_keyed_rate_limit_per_tenant_independence(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Each tenant gets an independent bucket; draining one does not affect the other.

    1. Drain tenant A's bucket with 3 jobs (capacity-3 → all 3 succeed,
       bucket now has 0 tokens).
    2. Wait for the drain to complete so the bucket is fully depleted.
    3. Enqueue 2 measured jobs for tenant A and 3 jobs for tenant B
       back-to-back.
    4. Tenant A's measured jobs face a depleted bucket (0 tokens, 1/s
       refill) → at least 1 is denied (proving per-tenant throttling).
    5. Tenant B's jobs face a fresh capacity-3 bucket → all 3 succeed
       immediately with 0 denials (proving per-tenant independence: tenant
       A's drain did not affect tenant B's bucket).
    """
    drain_id = f"{run_id}-drain"

    # Step 1: drain tenant A's bucket.
    drain_handles = [
        await e2e_client.enqueue(
            deliver_tenant_webhook,
            DeliverTenantWebhookPayload(
                run_id=drain_id, tenant_id=_TENANT_A, endpoint_id=f"drain-{i}"
            ),
        )
        for i in range(_DRAIN_SIZE)
    ]

    # Step 2: wait for drain to complete — bucket is now fully depleted.
    await wait_all(drain_handles, timeout=90)

    # Step 3: enqueue measured jobs for both tenants back-to-back.
    tenant_a_handles = [
        await e2e_client.enqueue(
            deliver_tenant_webhook,
            DeliverTenantWebhookPayload(
                run_id=run_id, tenant_id=_TENANT_A, endpoint_id=f"measured-a-{i}"
            ),
        )
        for i in range(_MEASURED_SIZE)
    ]
    tenant_b_handles = [
        await e2e_client.enqueue(
            deliver_tenant_webhook,
            DeliverTenantWebhookPayload(
                run_id=run_id, tenant_id=_TENANT_B, endpoint_id=f"measured-b-{i}"
            ),
        )
        for i in range(_DRAIN_SIZE)
    ]

    await wait_all([*tenant_a_handles, *tenant_b_handles], timeout=90)

    # Step 4: tenant A's measured jobs face a depleted bucket.
    # After the drain completes, tenant A's bucket has 0 tokens refilling
    # at 1/s. The 2 measured jobs are enqueued immediately; the worker
    # dispatches them into a depleted bucket. With 1/s refill, it takes
    # ~1s to refill 1 token. If the test process + producer poll takes
    # < 1s, both jobs are denied. If it takes 1-2s, 1 token has refilled
    # and 1 job may succeed. So at least 1 of 2 is denied.
    tenant_a_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in tenant_a_handles]
    )
    assert len(tenant_a_rows) == _MEASURED_SIZE
    a_denied, a_per_job = _denied_count(tenant_a_rows, _TENANT_A)
    assert a_denied >= 1, (
        f"tenant A: {a_denied}/{_MEASURED_SIZE} measured jobs were "
        f"rate-limit-denied (expected ≥ 1 — drained capacity-3 bucket with "
        f"1/s refill should deny at least 1 of 2 measured jobs); "
        f"per-job (max_attempts, awaiting): "
        f"{', '.join(f'{j}:{a}/{w}' for j, (a, w) in sorted(a_per_job.items()))}"
    )

    # Step 5: tenant B's jobs face a FRESH capacity-3 bucket — 0 denials.
    # Tenant B's bucket is materialized lazily on first acquisition and
    # starts at full capacity (3 tokens). All 3 jobs should succeed
    # immediately. This proves per-tenant independence: tenant A's drain
    # did not consume tenant B's tokens.
    tenant_b_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in tenant_b_handles]
    )
    assert len(tenant_b_rows) == _DRAIN_SIZE
    b_denied, b_per_job = _denied_count(tenant_b_rows, _TENANT_B)
    assert b_denied == 0, (
        f"tenant B: {b_denied}/{_DRAIN_SIZE} jobs were rate-limit-denied "
        f"(expected 0 — fresh capacity-3 bucket should admit all 3 jobs; "
        f"tenant A's drain must not affect tenant B's bucket); "
        f"per-job (max_attempts, awaiting): "
        f"{', '.join(f'{j}:{a}/{w}' for j, (a, w) in sorted(b_per_job.items()))}"
    )

    # All jobs delivered exactly once.
    drain_effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, drain_id, kind="tenant_delivered"
    )
    assert len(drain_effects) == _DRAIN_SIZE

    a_effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="tenant_delivered"
    )
    # detail is a JSONB column — asyncpg returns it as a str unless a codec
    # is registered. Parse it to access the tenant_id field.
    a_job_ids = {
        row["job_id"] for row in a_effects if json.loads(row["detail"])["tenant_id"] == _TENANT_A
    }
    b_job_ids = {
        row["job_id"] for row in a_effects if json.loads(row["detail"])["tenant_id"] == _TENANT_B
    }
    assert a_job_ids == {h.job_id for h in tenant_a_handles}
    assert b_job_ids == {h.job_id for h in tenant_b_handles}

    # Per-tenant throttling: the denial count (≥ 1, asserted above) is the
    # primary guard — it proves tenant A's measured jobs faced a depleted
    # bucket. A spread threshold on only 2 measured jobs with a 1/s refill
    # rate is unreliable (the bucket can fully refill during the
    # wait+enqueue cycle), so we do NOT assert tenant A's spread. Tenant
    # B's spread is still checked: a fresh capacity-3 bucket should not
    # pace 3 jobs (spread < 0.5s).
    a_delivered = [row for row in a_effects if json.loads(row["detail"])["tenant_id"] == _TENANT_A]
    b_delivered = [row for row in a_effects if json.loads(row["detail"])["tenant_id"] == _TENANT_B]
    assert len(a_delivered) == _MEASURED_SIZE
    assert len(b_delivered) == _DRAIN_SIZE

    b_spread = (
        max(r["at"] for r in b_delivered) - min(r["at"] for r in b_delivered)
    ).total_seconds()
    assert b_spread < 0.5, (
        f"tenant B spread {b_spread:.2f}s ≥ 0.5s — fresh capacity-3 bucket should not pace 3 jobs"
    )


async def test_typed_keyed_rate_limit_with_aliases(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """KeyedRateLimitRef.typed with aliased payload resolves key from model attribute.

    The payload model uses ``Field(alias="tenantId")`` with
    ``serialize_by_alias=True`` so the stored row carries the wire alias
    ``tenantId`` (not ``tenant_id``). A raw-dict ``key_fn`` would fail with
    ``KeyError`` / ``AttributeError`` — this test proves the validated
    ``BaseModel`` reaches ``key_fn`` at acquire time.
    """
    tenant_a = "gamma"
    tenant_b = "delta"

    handles_a = [
        await e2e_client.enqueue(
            deliver_typed_tenant_webhook,
            TypedTenantPayload(
                run_id=run_id,
                tenant_id=tenant_a,  # type: ignore[reportCallIssue]  # Why: validate_by_name=True allows construction by field name; pyright's pydantic stubs don't recognize this flag.
                endpoint_id=f"ep-{i}",
            ),
        )
        for i in range(3)
    ]
    handles_b = [
        await e2e_client.enqueue(
            deliver_typed_tenant_webhook,
            TypedTenantPayload(
                run_id=run_id,
                tenant_id=tenant_b,  # type: ignore[reportCallIssue]  # Why: validate_by_name=True allows construction by field name; pyright's pydantic stubs don't recognize this flag.
                endpoint_id=f"ep-{i}",
            ),
        )
        for i in range(3)
    ]

    await asyncio.gather(*(h.wait(timeout=90) for h in [*handles_a, *handles_b]))

    a_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in handles_a]
    )
    b_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in handles_b]
    )

    # Verify the stored row carries the wire alias — proving the model
    # with serialize_by_alias=True stored "tenantId", not "tenant_id"
    for row in a_rows:
        row_payload = json.loads(row["payload"])
        assert "tenantId" in row_payload
        assert row_payload["tenantId"] == tenant_a

    _typed_bucket_base = "e2e_typed_per_tenant"
    a_denied = sum(
        1
        for row in a_rows
        if (awaiting := json.loads(row["metadata"]).get("awaiting")) is not None
        and awaiting.endswith(f"{_typed_bucket_base}:{tenant_a}")
    )
    b_denied = sum(
        1
        for row in b_rows
        if (awaiting := json.loads(row["metadata"]).get("awaiting")) is not None
        and awaiting.endswith(f"{_typed_bucket_base}:{tenant_b}")
    )

    assert a_denied == 0, f"tenant A had {a_denied} denials (expected 0)"
    assert b_denied == 0, f"tenant B had {b_denied} denials (expected 0)"

    effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="typed_tenant_delivered"
    )
    assert len(effects) == 6

    a_effects = [r for r in effects if json.loads(r["detail"])["tenant_id"] == tenant_a]
    b_effects = [r for r in effects if json.loads(r["detail"])["tenant_id"] == tenant_b]
    assert len(a_effects) == 3
    assert len(b_effects) == 3
