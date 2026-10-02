"""The fleet demo's actor registry.

One module, every flagship surface the demo walks:

- ``ship_order``          — the happy path: enqueue → dispatch → succeed.
- ``token_metered``       — Redis-backed **token bucket** (2 capacity, 1/s refill).
- ``window_metered``      — Redis-backed **GCRA sliding window** (3 per 15s).
- ``long_haul``           — a long job that polls cooperative cancellation;
  the operator-cancel act and the SIGTERM deploy act both drive it.
- ``cron_digest``         — a cron schedule (every 15s) with a payload factory;
  the catch-up window's skip and its sequential catch-up crawl both act on it.
- ``cron_greedy_one`` / ``cron_greedy_two`` — two schedules whose payload
  factories each burn ~4s of the cron tick's ~4.5s funded budget. The tick
  can fund ONE of them; the other is budget-DEFERRED every greedy tick
  (the ``cron-fire-budget-deferred`` event), never struck.
- ``offline_meter``       — bound to queue ``offline``, which no worker
  serves: the stranded-work finding the doctor act stages.
- ``ghost_job``           — present ONLY in ``SICK_ACTORS`` (the doctor
  act's registry), never in the worker's registry, so it has no stored
  actor_config row: the "NEVER DISPATCHES" finding family.

Rate-limit primitives register on the module-level ``registry`` singleton
at import time, so the worker picks them up before dispatch begins.
"""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel

from taskq import ActorRef, JobContext, actor, cron
from taskq.ratelimit import SlidingWindow, TokenBucket, registry

# ── Rate-limit primitives (Redis-backed; both algorithms) ──────────────

registry.register(
    TokenBucket(
        name="fleet_token",
        capacity=2,
        refill_per_second=1.0,
        backend="redis",
    )
)

registry.register(
    SlidingWindow(
        name="fleet_window",
        limit=3,
        window=timedelta(seconds=15),
        backend="redis",
    )
)


# ── The happy path ──────────────────────────────────────────────────────


class ShipPayload(BaseModel):
    order_id: str


class ShipResult(BaseModel):
    order_id: str
    lanes: int


@actor(name="ship_order", queue="fleet")
async def ship_order(payload: ShipPayload) -> ShipResult:
    """A fast, ordinary job: pending → running → succeeded in ~0.2s."""
    await asyncio.sleep(0.2)
    return ShipResult(order_id=payload.order_id, lanes=3)


# ── The rate-limited actors ─────────────────────────────────────────────


class MeterPayload(BaseModel):
    batch: str


@actor(name="token_metered", queue="fleet", rate_limits=["fleet_token"])
async def token_metered(payload: MeterPayload) -> None:
    """Runs 1s behind the token bucket: 2 immediate, then 1 dispatch/s."""
    await asyncio.sleep(1)


@actor(name="window_metered", queue="fleet", rate_limits=["fleet_window"])
async def window_metered(payload: MeterPayload) -> None:
    """Runs 1s behind the GCRA sliding window: 3 per 15s fleet-wide."""
    await asyncio.sleep(1)


# ── The long job the cancel acts drive ─────────────────────────────────


class LongHaulPayload(BaseModel):
    seconds: int = 45


@actor(name="long_haul", queue="fleet")
async def long_haul(payload: LongHaulPayload, ctx: JobContext[LongHaulPayload]) -> None:
    """Runs ~45s, reporting progress and polling the cooperative-cancel flag.

    The ownership contract's actor side lives here: on an operator cancel
    the checkpoint observes the flag and ``ctx.check_cancelled()`` raises
    the cooperative unwind — the worker routes that terminal write by the
    cancel ORIGIN, so an operator cancel lands ``cancelled`` (the row keeps
    its cancel bookkeeping), while the same unwind under a deploy SIGTERM
    is instead interrupted and RELEASED back to the fleet: a deploy never
    terminalises a job row.
    """
    deadline = time.monotonic() + payload.seconds
    step = 0
    while time.monotonic() < deadline:
        if ctx.should_abort():
            ctx.log.info(
                "long-haul unwinding at the cancel checkpoint",
                step=step,
                origin_seen="cancel flag observed",
            )
        # The checkpoint: raises the cooperative unwind when a cancel has
        # been requested. Returning without it would read as success.
        ctx.check_cancelled()
        step += 1
        await ctx.progress(
            step=step,
            percent=round(step * 0.5 / payload.seconds * 100, 1),
            detail=f"lane {step}",
        )
        await asyncio.sleep(0.5)
    ctx.log.info("long-haul finished its run")


# ── The cron fleet ──────────────────────────────────────────────────────


class DigestPayload(BaseModel):
    digest_id: str


@actor(name="cron_digest", queue="fleet")
async def cron_digest(payload: DigestPayload) -> None:
    """The schedule the catch-up acts drive: every 15s via a payload factory."""
    pass


async def digest_payload_factory() -> DigestPayload:
    """A zero-arg factory: the cron tick calls it to mint the payload."""
    return DigestPayload(digest_id=f"auto-{datetime.now(UTC).strftime('%H%M%S')}")


class GreedyPayload(BaseModel):
    batch: str


@actor(name="cron_greedy_one", queue="fleet")
async def cron_greedy_one(payload: GreedyPayload) -> None:
    pass


@actor(name="cron_greedy_two", queue="fleet")
async def cron_greedy_two(payload: GreedyPayload) -> None:
    pass


def _burn_budget() -> GreedyPayload:
    """A slow-SUCCESSFUL factory: sleeps ~4s of the tick's ~4.5s funded budget.

    The tick can fund ONE call of this shape. Whichever greedy schedule the
    tick plans SECOND is refused (the leftover 0.45s is below the minimum
    fundable grant, a quarter of the funded budget) and is budget-DEFERRED:
    its next_fire_at advances one cadence, no strike is recorded, and the
    ``cron-fire-budget-deferred`` event names it.
    """
    time.sleep(4.0)
    return GreedyPayload(batch=f"greedy-{datetime.now(UTC).strftime('%H%M%S')}")


def greedy_one_factory() -> GreedyPayload:
    return _burn_budget()


def greedy_two_factory() -> GreedyPayload:
    return _burn_budget()


cron(
    "* * * * * */15",
    "cron_digest",
    payload_factory="examples.fleet_demo.actors.digest_payload_factory",
    name="digest",
)
cron(
    "* * * * * */5",
    "cron_greedy_one",
    payload_factory="examples.fleet_demo.actors.greedy_one_factory",
    name="greedy-one",
)
cron(
    "* * * * * */5",
    "cron_greedy_two",
    payload_factory="examples.fleet_demo.actors.greedy_two_factory",
    name="greedy-two",
)


# ── The sick-config actors (doctor act only) ────────────────────────────


class MeterPayload2(BaseModel):
    n: int = 1


@actor(name="offline_meter", queue="offline")
async def offline_meter(payload: MeterPayload2) -> None:
    """Bound to queue 'offline' — no worker in the demo serves that queue."""


@actor(name="ghost_job", queue="fleet")
async def ghost_job(payload: MeterPayload2) -> None:
    """Registered in the doctor act's registry but never in the worker's:
    no stored actor_config row, so it can never dispatch."""


def _refs(*actors_: Any) -> dict[str, ActorRef[Any, Any]]:
    return {a.name: a for a in actors_}


#: The worker's registry: everything the fleet serves. `offline_meter` and
#: `ghost_job` are deliberately absent — their absence IS the demo.
FLEET_ACTORS: dict[str, ActorRef[Any, Any]] = _refs(
    ship_order,
    token_metered,
    window_metered,
    long_haul,
    cron_digest,
    cron_greedy_one,
    cron_greedy_two,
)

#: The doctor act's registry: the fleet's actors PLUS the sick pair. The
#: worker never sees this dict, so `ghost_job` never gets a stored row and
#: `offline_meter`'s queue is never subscribed.
SICK_ACTORS: dict[str, ActorRef[Any, Any]] = {
    **FLEET_ACTORS,
    "offline_meter": offline_meter,
    "ghost_job": ghost_job,
}
