"""Scenario 2's definitions: three tenants' runs on ONE actor, at three
priorities. The worker carries this module (the ``--actors`` reference);
the scenario's own process creates the runs and reads the rows.

The step's body reads the run's input (the tenant and its item ride the
input, the same for every node of the run); the flow is one step long on
purpose: the claim order of its 150 rows is the whole story."""

from __future__ import annotations

from pydantic import BaseModel

from taskq import actor
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

app = WorkflowApp()


class TenantWork(BaseModel):
    """One run's input: which tenant sent the item."""

    tenant: str
    item: str


class TenantOutcome(BaseModel):
    """The step's result: the work done for one tenant's item."""

    tenant: str
    item: str


@app.actor(name="wf_tenant_worker", queue="grounds-tenant")
async def process_item(ctx: StepContext) -> TenantOutcome:
    work = TenantWork.model_validate(ctx.input)
    return TenantOutcome(tenant=work.tenant, item=work.item)


@app.workflow("ground_tenant")
def ground_tenant() -> Promise[object]:
    return build(
        step(process_item, key="process", actor="wf_tenant_worker", queue="grounds-tenant")
    )


class Ping(BaseModel):
    """A plain job the registry requires the worker to carry (the
    worker's boot refuses an empty actor registry); nothing in the
    scenario enqueues it."""

    note: str = ""


@actor(name="grounds_ping", queue="grounds-tenant")
async def grounds_ping(payload: Ping) -> Ping:
    return payload


#: The worker's ``--actors`` reference.
ACTORS: dict[str, object] = {"grounds_ping": grounds_ping}
