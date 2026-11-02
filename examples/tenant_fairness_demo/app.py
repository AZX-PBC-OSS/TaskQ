"""The multi-tenant workflow demo's definitions: TWO tenants' runs, ONE
keyed rate limit, ONE round_robin queue.

The keyed ref is declared on the map (`rate_limits=`): each item body's
row carries ITS OWN tenant's concrete bucket name
(`tenant-api:<tenant>`), resolved from the item's payload at the fork.
At claim, the per-key bucket serializes each tenant's items while the
tenants run beside each other, and the round_robin queue interleaves
the two runs' cohorts (each run is its own `fairness_key` cohort — the
rows are born stamped).
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.workflows import (
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)

#: The queue every row of the demo rides — mode set to `round_robin` by
#: the runner script before the runs start.
QUEUE = "tenants"

#: How many items each tenant's run forks.
ITEMS_PER_TENANT = 3


class TenantRun(BaseModel):
    tenant: str


class Item(BaseModel):
    tenant_id: str
    seq: int


class Processed(BaseModel):
    tag: str


#: The keyed rate limit: one token per tenant bucket, refilling at
#: 0.5/s — a tenant's item that arrives with the bucket empty (the
#: previous item spent it 0.2 s ago) denies (snoozes budget-free) and
#: re-claims on a later round, after the token returns.
TENANT_API = KeyedRateLimitRef.typed(
    Item,
    base_name="tenant-api",
    key_fn=lambda p: p.tenant_id,
    capacity=1,
    refill_per_second=0.5,
    backend="memory",
)


tenant_app = WorkflowApp()


async def source(ctx: StepContext) -> list[Item]:
    # The RUN'S OWN INPUT (create_flow's `input=`) names this run's
    # tenant — two runs of the one graph, one tenant each.
    params = TenantRun.model_validate(ctx.input)
    return [Item(tenant_id=params.tenant, seq=i) for i in range(ITEMS_PER_TENANT)]


async def process_item(ctx: StepContext, value: Item) -> Processed:
    # The body's own latency: long enough that the tenant's capacity-1
    # bucket is empty when the next item of the same tenant claims.
    await asyncio.sleep(0.2)
    return Processed(tag=f"{value.tenant_id}:{value.seq}")


async def last(ctx: StepContext, value: list[Processed]) -> Processed:
    _ = ctx
    return value[-1]


@tenant_app.workflow("tenant_pipeline")
def tenant_pipeline() -> Promise[object]:
    source_step = step(source, key="source", queue=QUEUE)
    mapped = map_source(source_step, process_item, queue=QUEUE, rate_limits=[TENANT_API])
    return build(step(last, mapped, key="last", queue=QUEUE))
