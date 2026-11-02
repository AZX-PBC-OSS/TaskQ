"""THE MAX_PENDING IMPOSSIBILITY PINS (C16 — the H-A6 verdict, proven).

THE QUESTION (the repo's law: a documented omission ships only with a
red-proven impossibility pin behind it — an assertion is not a proof):
can a CLAIM-SIDE ``max_pending`` throttle for wf cohorts be BUILT, or is
H-A6's removal structurally forced?

THE VERDICT, EARNED BELOW: **IMPOSSIBLE — a genuine deadlock.** The pin
demonstrates the mechanism mechanically, on the real engine, with the
mandate's OWN minimal gate implementation:

    the gate: per cohort, when the cohort's pending count >= max_pending,
    those rows are not claimed this pass (delay semantics, never an
    error).

THE FIXED-POINT MECHANISM (each leg asserted by a pin):

* the wf engine's inserts bypass the enqueue admission — the map fork
  materializes the whole fan-out in the source's finalize
  (``ChildSpec(actor=node.actor, ...)`` rows), and the drain_outbox /
  escalation inserts ride the same bypass. The gate has no insert-side
  say: the backlog GROWS PAST the cap while nothing runs (pin 1).
* pending drains ONLY by claiming (pending → running) — the finalize's
  ``deps_pending`` decrement rides the claimed row's terminal; no sweep
  moves a pending row anywhere (the reaper owns RUNNING leases, the
  retention sweep owns TERMINAL rows, the join-rederive only fires
  already-terminal parents' children).
* so at ``pending >= max`` the gate holds every row, and the held rows
  keep the count at its fixed point: ``pending' = pending - claims`` and
  ``claims = 0`` exactly when ``pending >= max``. The condition keys to
  the very level only the gate's own release would lower (pin 2: the
  count is identical across ticks, zero bodies ran, the join's
  ``deps_pending`` never decremented, the run never terminalizes).
* the CONTROL: the same flow without the gate reaches terminal (the
  wedge is the gate's own fixed point, not the engine's) (pin 3).
* the INERT arm: the same gate above the flow's total row count never
  fires and the flow completes — the gate's active domain IS its wedge
  domain. A gate that never wedges (an ordering hold, a per-pass leak)
  never bounds the backlog — it is the claim's existing batch limit
  wearing a mask, the DEAD KNOB H-A6 removed, not a max_pending (pin 4).

THE RED THAT PROVED IT: the build attempt's own pin — "a fan-out WIDER
than max_pending completes to terminal" with this exact gate installed —
was written first and watched fail (the drive burns ``max_ticks`` with
zero claims). That red is the anti-deadlock proof's impossibility; the
committed pins below are its inversion.

THE FLEET SEAT: the same mechanism at the fleet's claim (``dispatch_batch``
— whose per-actor arithmetic reads ``actor_config.max_concurrent``, never
``max_pending`` — the H-A6 premise, grep-verified): the fleet claim is
the SAME drain (pending → running, the only one), so a gate keyed to the
cohort's pending level has the same fixed point there. The runner's
claim seat demonstrates it minimally; the seat is not the mechanism.
Materialization-side bounded channels are a different face, not tried
here — they bound inserts by stalling the source's finalize at the cost
of the source's own lease budget, and give producers no admission seat.

THE SURFACE STAYS CLOSED: ``test_max_pending_is_off_the_wf_surface``
(the H-A6 pin) keeps passing — the omission now rides a red-proven
impossibility instead of an assertion.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)

#: The fan-out's width — WIDER than the gate's max_pending (the
#: anti-deadlock proof's own shape).
WIDTH = 12

#: The gate's cap — the fan-out exceeds it at the fork.
MAX_PENDING = 10


class Batch(BaseModel):
    size: int


class _GatedFanOutApp:
    """The repro's app: a source fanning out WIDTH items through a map;
    the items + the join + the source share ONE cohort (the fork stamps
    ``actor=node.actor`` on every child). ``ran`` records the item bodies'
    executions. Each instance registers its OWN workflow name — the D1
    definition registry is process-global and the per-instance bodies are
    distinct closures (a same-name re-registration is the refused
    shadow)."""

    _sequence = 0

    def __init__(self) -> None:
        _GatedFanOutApp._sequence += 1
        self.name = f"gated_fanout_flow_{_GatedFanOutApp._sequence}"
        self.ran: list[str] = []
        self.app = WorkflowApp()

        async def source(ctx: StepContext, params: Batch) -> list[str]:
            return [f"i{i}" for i in range(params.size)]

        async def item(ctx: StepContext, name: str) -> str:
            self.ran.append(name)
            return name

        @self.app.workflow(self.name)
        def gated_fanout_flow() -> Promise[object]:
            source_p = step(source, Batch(size=WIDTH), key="source", actor="wf-fan")
            joined = map_source(source_p, item)  # the children ride the source's cohort
            return build(joined)

    def runner(self, pool: asyncpg.Pool, schema: str) -> FlowRunner:
        return FlowRunner(self.app.get(self.name), pool, schema)


class ClaimSideMaxPendingGate:
    """THE MANDATE'S OWN MINIMAL GATE, at the runner's claim seat: per
    cohort, when the cohort's pending count >= max_pending, those rows
    are not claimed this pass — the row keeps its pending status (delay
    semantics, never an error) and the next pass re-derives from the
    same count. Nothing else about the engine changes."""

    def __init__(self, pool: asyncpg.Pool, schema: str, max_pending: int) -> None:
        self._pool = pool
        self._schema = schema
        self._max = max_pending
        self.held_observations = 0

    async def claimable(self, runner: FlowRunner, flow_id: Any) -> list[dict[str, Any]]:
        rows = await FlowRunner._claimable(runner, flow_id)  # pyright: ignore[reportAttributeAccessIssue]  # Why: the gate wraps the runner's own claim seat — the unbound method is the seat's original.
        if not rows:
            return rows
        ids = [r["id"] for r in rows]
        async with self._pool.acquire() as conn:
            actor_rows = await conn.fetch(
                f'SELECT id, actor FROM "{self._schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: the schema identifier is the fixture's own; asyncpg cannot bind identifiers.
                ids,
            )
            actor_by_id = {str(r["id"]): str(r["actor"]) for r in actor_rows}
            actors = sorted(set(actor_by_id.values()))
            count_rows = await conn.fetch(
                f'SELECT actor, count(*) AS n FROM "{self._schema}".jobs '  # noqa: S608
                "WHERE actor = ANY($1::text[]) AND status = 'pending' GROUP BY actor",
                actors,
            )
        pending_by_actor = {str(r["actor"]): int(r["n"]) for r in count_rows}
        kept: list[dict[str, Any]] = []
        for row in rows:
            actor = actor_by_id.get(str(row["id"]))
            if actor is not None and pending_by_actor.get(actor, 0) >= self._max:
                self.held_observations += 1  # THE HOLD: pending >= max → not claimed this pass
                continue
            kept.append(row)
        return kept


def _install_gate(
    runner: FlowRunner, pool: asyncpg.Pool, schema: str, max_pending: int
) -> ClaimSideMaxPendingGate:
    """Arm the gate on THIS runner's claim seat (instance-level: the pin's
    own installation, no product code touched)."""
    gate = ClaimSideMaxPendingGate(pool, schema, max_pending)

    async def gated_claimable(flow_id: Any) -> list[dict[str, Any]]:
        return await gate.claimable(runner, flow_id)

    runner._claimable = gated_claimable  # type: ignore[method-assign]  # Why: the pin installs the mandate's gate at the claim seat — the seat is the seam.
    return gate


async def _cohort_pending(pool: asyncpg.Pool, schema: str, actor: str) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status = 'pending'",  # noqa: S608  # Why: the schema identifier is the fixture's own; asyncpg cannot bind identifiers.
                actor,
            )
        )


async def _node_row(pool: asyncpg.Pool, schema: str, flow_id: Any, step_key: str) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT id, status, deps_pending FROM "{schema}".jobs '  # noqa: S608
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
            flow_id,
            step_key,
        )
    assert row is not None, f"the {step_key!r} row must exist"
    return dict(row)


# ═══════════════════════════════════════════════════════════════════════════════
# The pins
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.integration
async def test_the_fan_outs_inserts_bypass_the_gate_and_the_admission(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """PIN 1 — THE BYPASS RECEIPT: with the gate ARMED at max_pending=10,
    the engine's fork still materializes the whole 12-wide fan-out — the
    cohort's pending count grows PAST the cap while zero item rows are
    ever claimed. The insert side never asks the gate (and never asks
    the enqueue admission): the backlog the gate was meant to bound is
    the one thing it cannot touch."""
    harness = _GatedFanOutApp()
    runner = harness.runner(wf_pool, wf_schema)
    claim = await runner.create_flow()
    _gate = _install_gate(runner, wf_pool, wf_schema, MAX_PENDING)

    # One tick: the source claims (pending = 1 < 10), runs, and its
    # finalize forks the 12 items.
    await runner.tick(claim.flow_id)

    pending = await _cohort_pending(wf_pool, wf_schema, "wf-fan")
    assert pending == WIDTH + 1, (
        f"the fork materialized the whole fan-out + the join row while the "
        f"gate was armed at {MAX_PENDING}: the engine's inserts bypass the "
        f"gate AND the enqueue admission (the cohort's pending = {pending})"
    )
    assert harness.ran == [], "no item body ran — the gate has no insert-side say"


@pytest.mark.integration
async def test_the_claim_side_gate_wedges_the_join_at_a_fixed_point(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """PIN 2 — THE IMPOSSIBILITY CORE: the mandate's own gate, armed at
    max_pending=10 against a 12-wide fan-out. The run NEVER reaches
    terminal: zero item bodies run, the cohort's pending count is a
    FIXED POINT across ticks (the gate holds every claim and claims are
    pending's only drain), the join's deps_pending never decrements, the
    root stays running. THE WEDGE — mechanically, not rhetorically."""
    harness = _GatedFanOutApp()
    runner = harness.runner(wf_pool, wf_schema)
    claim = await runner.create_flow()
    gate = _install_gate(runner, wf_pool, wf_schema, MAX_PENDING)

    # The source's pass (the fork lands, the gate closes behind it).
    await runner.tick(claim.flow_id)
    pending_after_fork = await _cohort_pending(wf_pool, wf_schema, "wf-fan")
    assert pending_after_fork >= MAX_PENDING, "the repro's precondition: pending >= max"

    # THE WEDGED DRIVE: a bounded drive never terminalizes.
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=30, tick=0.01)
    assert outcome == "max_ticks", (
        f"the gated run wedged: the drive never reached terminal ({outcome})"
    )

    assert harness.ran == [], "zero item bodies ran — the gate held every claim"
    pending_after = await _cohort_pending(wf_pool, wf_schema, "wf-fan")
    assert pending_after == pending_after_fork, (
        "the pending count is a FIXED POINT: claims are pending's only "
        "drain and the gate holds exactly the claims that would drain it"
    )
    join = await _node_row(wf_pool, wf_schema, claim.flow_id, "source.join")
    assert join["deps_pending"] == WIDTH, (
        "the join's counter never decremented — a finalize only runs for a "
        "claimed row, and no row is ever claimed"
    )
    assert join["status"] == "pending", "the join never fired"
    assert gate.held_observations > 0, "the throttle HELD rows (the hold arm fired every pass)"


@pytest.mark.integration
async def test_the_same_flow_without_the_gate_reaches_terminal(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """PIN 3 — THE CONTROL: the identical flow with NO gate completes.
    The wedge is the gate's own fixed point, never the engine's."""
    harness = _GatedFanOutApp()
    runner = harness.runner(wf_pool, wf_schema)
    claim = await runner.create_flow()

    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=60, tick=0.01)
    assert outcome == "terminal"
    assert sorted(harness.ran) == sorted(f"i{i}" for i in range(WIDTH)), (
        "every item body ran exactly the fan-out"
    )
    join = await _node_row(wf_pool, wf_schema, claim.flow_id, "source.join")
    assert join["deps_pending"] == 0
    assert join["status"] == "succeeded", "the join fired and terminalized"


@pytest.mark.integration
async def test_the_gate_above_the_cap_is_inert(wf_pool: asyncpg.Pool, wf_schema: str) -> None:
    """PIN 4 — THE INERT ARM: the same gate armed ABOVE the flow's total
    row count never fires, and the flow completes. With pin 2: the
    gate's active domain IS its wedge domain — a claim-side gate that
    avoids the wedge cannot bound the backlog (it is the claim's
    existing batch limit wearing a mask, the dead knob H-A6 removed)."""
    harness = _GatedFanOutApp()
    runner = harness.runner(wf_pool, wf_schema)
    claim = await runner.create_flow()
    gate = _install_gate(runner, wf_pool, wf_schema, WIDTH + 10)  # above the flow's total

    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=60, tick=0.01)
    assert outcome == "terminal"
    assert len(harness.ran) == WIDTH
    assert gate.held_observations == 0, "the gate never fired below the wedge threshold"
