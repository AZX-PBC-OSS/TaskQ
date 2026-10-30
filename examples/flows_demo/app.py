"""The flows demo's workflow definitions — ONE graph, the whole story.

The graph (``flows_demo``) demonstrates, on one run:

1. **The SOURCE fan-out + the failing item's COLLECT** — the source step
   emits a batch of items; the map fans each item out as its OWN job;
   one item is ARMED to fail (every attempt); the map's edges declare
   ``on_failure="collect"`` so the failure fans in as a typed
   :class:`~taskq.workflows._types.FailureInfo` on the map join row's
   ``metadata.failures`` and THE FLOW STILL COMPLETES (the surviving
   items publish; the failure is REPORTED, never a cascade).
2. **The HITL gate** — the review step waits on a typed
   :class:`Approval` payload (``ctx.wait_signal`` + a ``GateDecl``);
   the node takes the HELD representation and the run pauses until the
   typed resolve arrives (the admin's Resolve form, or
   ``HitlClient.resolve`` — the same rows).
3. **The durable timer** — the cooldown step sleeps on
   ``ctx.wait_until`` (a hold whose resolver is the CLOCK): the wake
   fires from the sweep's timer arm, not from a process-local sleep.
4. **The CAPPED stage** — the publish step declares the capacity
   surface's terms on the wiring
   (``max_concurrent=1, reservations=[…], result_ttl=…``)
   and guards a deliberately not-thread-safe resource: the module-level
   overlap guard raises if two of its bodies EVER overlap (on a fleet
   the cap keeps that impossible; in-process it is the tripwire that
   proves the demo never crossed the line).
5. **The typed sink** — the terminal report derives from the run's own
   rows; the narration step rides ``sink()`` (produced, recorded,
   fire-and-forget).

The capacity terms ride the HANDLE face —
``@flows_app.actor(max_concurrent=1, reservations=[…], result_ttl=…)``
— and flow from the handle into the
node at wiring time; the boot projection carries them into
``actor_config`` (``run_demo`` lands the rows the same way the worker
boot does and prints them). The reservation's schema binding lands at
:func:`configure` (the demo's launch seam — the declaration at import
cannot know the deployment's schema).

Run commands + the WHAT-TO-OBSERVE list: ``README.md``.
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.workflows import (
    Expired,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    sink,
    step,
)
from taskq.workflows.api import GateDecl

__all__ = [
    "DEMO_ITEMS",
    "DOOMED_ITEM",
    "PDF_RESERVATION",
    "PDF_RESERVATION_NAME",
    "SERIAL_GUARD",
    "Approval",
    "BatchSpec",
    "Digest",
    "ItemOut",
    "configure",
    "flows_app",
]

#: The demo's batch: SEVERAL items, one of them armed to fail every
#: attempt (the collect's demonstration — the surviving four publish,
#: the failure is reported by the join row).
DEMO_ITEMS: list[str] = ["item-alpha", "item-beta", "item-gamma", "item-delta"]

#: THE DOOMED ITEM — the batch's one deliberate failure. Its fan-in IS
#: the collect: a typed ``FailureInfo`` envelope on the map join row.
DOOMED_ITEM = "item-999-doomed"

#: The demo's batch, doomed item included (the wiring's declared
#: default — the source body reads ``ctx.input`` when a caller passes
#: one, the wired batch when it does not).
ITEMS: list[str] = sorted([*DEMO_ITEMS, DOOMED_ITEM])

#: The capped stage's reservation — the STRICT slot (one slot: the
#: not-thread-safe resource has exactly one seat). The DECLARATION at
#: import binds the settings-default schema (no import-time settings —
#: the admin_app doctrine); :func:`configure` rebinds the handle's
#: declared reservation to the demo's schema BEFORE anything compiles —
#: the wiring, the projection and the bootstrap's ensure-slots filter
#: all read the instance's own schema against the deployment's
#: settings.
PDF_RESERVATION_NAME = "flows_demo_pdf_gadget"

#: The import-time declaration (the settings-default schema binding —
#: see the comment above).
PDF_RESERVATION = ConcurrencyReservation(
    name=PDF_RESERVATION_NAME,
    slots=1,
    lease=60.0,
)

_pdf_gadget: ConcurrencyReservation | None = None


def configure(schema: str) -> ConcurrencyReservation:
    """Bind the demo's schema-dependent pieces (the reservation's slot
    rows live in the demo's schema). Call BEFORE ``flows_app.get(...)``
    — the wiring compiles fresh on every ``get`` and reads the handle's
    declared terms at wiring time."""
    global _pdf_gadget  # Why: the module IS the demo's config point; the schema is unknowable at import (no import-time settings — the admin_app doctrine).
    _pdf_gadget = ConcurrencyReservation(
        name=PDF_RESERVATION_NAME,
        slots=1,
        lease=60.0,
        schema=schema,
    )
    publish_capped.reservations = [_pdf_gadget]
    return _pdf_gadget


# ── the payload contract (the typed doors' shapes) ──────────────────────


class BatchSpec(BaseModel):
    """The source's declared batch (the wired default input)."""

    items: list[str]


class ItemOut(BaseModel):
    """One enriched item."""

    item: str
    words: int


class Approval(BaseModel):
    """The HITL gate's typed payload — the admin's Resolve form
    validates against THIS model (the gate's declared door)."""

    verdict: str  # "approve" | "reject"
    note: str = ""


class Digest(BaseModel):
    """The typed terminal report — DERIVED from the run's own rows
    (the published items are the map join's surviving collect; the
    doomed item's story lives on the join row's ``metadata.failures``)."""

    published: list[str]
    review_note: str
    cooldown: str


#: The review gate's COMPILE declaration (the Mermaid face + the typed
#: resolve door's registry entry). The RUNTIME deadline is the wait
#: site's own ``timeout_s`` — keep the two equal on purpose.
APPROVAL_GATE = GateDecl(
    name="Approval",
    payload_models=(Approval,),
    timeout_s=300.0,
)


# ── the not-thread-safe resource's guard (the cap's tripwire) ───────────


class OverlapGuard:
    """A fake not-thread-safe resource's door: the counter increments on
    entry, and a SECOND entry while one is in flight raises — the
    resource's own "two bodies overlapped" scream. A ``max_concurrent=1``
    stage keeps this at zero by construction; the demo carries it so a
    regression (the cap dropped from the wiring, the claim arithmetic
    changed) is a LOUD failure, never a silent admission."""

    def __init__(self) -> None:
        self.in_flight = 0
        #: The stages that ever overlapped (empty is the demo's law).
        self.tripped: list[str] = []
        #: How many bodies entered (the README's counter observation).
        self.entries = 0

    async def enter(self, stage: str) -> None:
        self.entries += 1
        self.in_flight += 1
        try:
            if self.in_flight > 1:
                self.tripped.append(stage)
                raise RuntimeError(
                    f"the not-thread-safe resource was entered twice ({stage}) — "
                    "the max_concurrent=1 cap did not hold"
                )
            await asyncio.sleep(0.05)
        finally:
            self.in_flight -= 1


SERIAL_GUARD = OverlapGuard()


# ── the bodies ───────────────────────────────────────────────────────────


async def fanout_source(ctx: StepContext, params: BatchSpec | None = None) -> list[str]:
    """The SOURCE: the run's own input is the truth (``ctx.input``
    carries the caller's batch); the wired batch is the default."""
    raw = ctx.input if ctx.input is not None else (params.model_dump() if params else None)
    return sorted(BatchSpec.model_validate(raw).items) if raw is not None else []


async def enrich_item(ctx: StepContext, item: str) -> ItemOut:
    """The map's per-item body — the doomed item fails EVERY attempt
    (the map declares ``max_attempts=1``); the survivors enrich."""
    await SERIAL_GUARD.enter(f"enrich:{item}")
    if item == DOOMED_ITEM:
        raise RuntimeError(f"the demo's armed failure ({item})")
    return ItemOut(item=item, words=len(item.split("-")))


async def await_approval(ctx: StepContext, ready: list[ItemOut]) -> str:
    """THE HITL GATE: the node holds on a typed ``Approval``; the
    resolve (the admin's form or ``HitlClient.resolve``) delivers the
    payload and the body resumes with it. The expiry is a value (the
    fail-close arm finishes with what we have)."""
    outcome = await ctx.wait_signal(
        (Approval,), timeout_s=300.0, reason="editorial review before publish (the demo)"
    )
    match outcome:
        case Approval() as decision:
            return f"{decision.verdict}: {decision.note}".strip(": ")
        case Expired():
            return "the review expired — shipping with what we have"


async def cooldown_body(ctx: StepContext, note: str) -> str:
    """THE DURABLE TIMER: the node takes the HELD representation and the
    SWEEP's timer arm (``fired_by='timer'``) wakes it at the deadline —
    the clock is the resolver, not a process-local sleep."""
    wake = await ctx.wait_until(0.5, key="cooldown")
    return f"{note} (cooldown fired_by={wake.fired_by})"


async def audit_note(ctx: StepContext, cooled: str) -> str:
    """The narration step — declared fire-and-forget (``sink``): its
    result is recorded and never consumed downstream."""
    return f"audit: the run reached the publish gate ({cooled})"


# ── the app + the wiring ────────────────────────────────────────────────

flows_app = WorkflowApp()


@flows_app.actor(
    name="publish_capped",
    queue="flows-pub",
    # THE CAPACITY SURFACE (the handle's own declaration): the capped
    # stage guards a not-thread-safe resource — one body at a time
    # (``max_concurrent=1``), a STRICT one-seat reservation, and a
    # result that outlives the run by an hour. The terms flow from the
    # handle into the node at wiring time; the boot projection carries
    # them into the cohort's ``actor_config`` row (the seed-only law —
    # the operator owns the row thereafter). The reservation's SCHEMA
    # binding lands at :func:`configure` (the deployment's launch seam).
    max_concurrent=1,
    reservations=[PDF_RESERVATION],
    result_ttl=3600.0,
)
async def publish_capped(ctx: StepContext, cooled: str, ready: list[ItemOut]) -> Digest:
    """THE CAPPED STAGE: the not-thread-safe resource's sole gatekeeper
    (the overlap guard screams if two bodies EVER overlap). The report
    derives from the wired collect — the survivors, named."""
    await SERIAL_GUARD.enter("publish")
    return Digest(
        published=sorted(it.item for it in ready),
        review_note=cooled,
        cooldown=cooled,
    )


@flows_app.workflow("flows_demo")
def flows_demo() -> Promise[object]:
    # THE SOURCE + THE MAP FAN-OUT: each item is its OWN job (per-item
    # ledger identity); the map's edges declare ``collect`` — the doomed
    # item's failure fans in as the typed partial result, the flow
    # completes.
    source = step(
        fanout_source,
        BatchSpec(items=ITEMS),
        key="source",
        actor="flows-source",
        queue="flows-src",
    )
    ready = map_source(
        source,
        enrich_item,
        queue="flows-src",  # the children ride the source's own cohort
        on_failure="collect",
        max_attempts=1,
    )

    # THE HITL GATE + THE DURABLE TIMER + THE SUNK NARRATION.
    review = step(
        await_approval,
        ready,
        key="review",
        actor="flows-review",
        queue="flows-review",
        gates=(APPROVAL_GATE,),
    )
    narration = step(
        audit_note,
        review,
        key="narration",
        actor="flows-review",
        queue="flows-review",
    )
    sink(narration)
    cooled = step(
        cooldown_body,
        review,
        key="cooldown",
        actor="flows-timer",
        queue="flows-timer",
    )

    # THE CAPPED STAGE (the capacity surface): the body is the
    # ``@flows_app.actor`` handle — the terms flow from the HANDLE into
    # the node (the wiring repeats nothing; the explicit step params
    # would OVERRIDE, never the reverse). The placement stays the
    # wiring's own decision (the handle's name is not the node's
    # actor).
    report = step(
        publish_capped,
        cooled,
        ready,
        key="report",
        actor="flows-pub",
        queue="flows-pub",
    )
    return build(report)
