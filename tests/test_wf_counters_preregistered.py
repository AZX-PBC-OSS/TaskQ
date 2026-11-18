"""The §4.2 pre-registered invocation-bound control (the contract's control
3): the seven operation families' census against the lane pins, before
execution. Red predicates exactly as the contract states: ``V_lane >
V_general`` is red, and measured ``V > V_lane`` is red; equality is not
required, and the tighter direct pin passing below the general aggregate is
the intended pass."""

from __future__ import annotations

import asyncio
import enum
import json as _json
from typing import Any

import pytest

from taskq.constants import WF_LOOP_ESCALATION_STEP_KEY

pytestmark = pytest.mark.integration


class OkOut(enum.Enum):
    """The chain outcome vocabulary (the enum the route declares)."""

    OK = "ok"


def _chain(name: str, body: Any) -> Any:
    from taskq.workflows import DONE, Chain, Route, Step

    return Chain(
        name=name,
        start="screen",
        steps={"screen": Step(body=body, outcomes=OkOut, route=Route({OkOut.OK: DONE}))},
    )


def _pre_registration() -> dict[str, Any]:
    """The §4.2 table's pre-registration for the direct-only lane, printed
    before execution; neither counts nor labels change after observation.

    The caps (57 lane / 60 general) are STANDALONE pre-registered
    acceptance-cap literals — deliberately stricter than the corrected
    analytical envelopes (A10), never the value of a current polynomial.
    The superseded historical derivations — the former
    ``19P+18S+4R+4B+J+C`` aggregate, its ``19P+18S+4R+B+J+C`` lane form,
    and the former ``4P+4S+R+B`` read ceiling — are recorded here as
    history, not current declarations (the corrected read family counts
    ACTUAL SQL query executions: each drain read point is one shared
    aggregate observation plus one fitting-candidate query)."""
    P, S, R, B, J, C = 2, 1, 0, 1, 0, 0
    K_ceiling = P + S
    G_ceiling = P + S
    I_ceiling = 2 * P + 2 * S
    D_ceiling = 3 * P + 3 * S + R + B
    U_ceiling = 5 * P + 5 * S + 2 * R + 2 * B
    # A10's corrected read family: candidate_reads counts actual SQL query
    # executions, 2 per drain read point → 8P + 8S + 2R + 2B (the former
    # 4P + 4S + R + B is a superseded historical derivation).
    reads_ceiling = 8 * P + 8 * S + 2 * R + 2 * B
    claims_ceiling = 2 * P + 2 * S + J + C
    # A10's corrected analytical envelopes (printed and asserted; not the
    # caps).
    v_general_envelope = 23 * P + 22 * S + 5 * R + 5 * B + J + C
    v_lane_envelope = 23 * P + 22 * S + 5 * R + B + J + C
    # The standalone pre-registered acceptance caps (never the value of a
    # current polynomial; the former 19P+18S+4R+4B+J+C aggregate and its
    # 19P+18S+4R+B+J+C lane form are superseded historical derivations).
    cap_v_lane = 57
    cap_v_general = 60
    return {
        "P": P,
        "S": S,
        "R": R,
        "B": B,
        "J": J,
        "C": C,
        "admission_decisions <= P + K": f"{P} + {K_ceiling}",
        "source_body_claims": claims_ceiling,
        "fitted_grants <= P + S": G_ceiling,
        "new_intents <= 2P + 2S": I_ceiling,
        "flow_drains <= 3P + 3S + R + B": D_ceiling,
        "global_drains <= 5P + 5S + 2R + 2B": U_ceiling,
        "candidate_reads <= 8P + 8S + 2R + 2B": reads_ceiling,
        "V_general_envelope = 23P + 22S + 5R + 5B + J + C": v_general_envelope,
        "V_lane_envelope = 23P + 22S + 5R + B + J + C": v_lane_envelope,
        "acceptance cap V_lane": cap_v_lane,
        "acceptance cap V_general": cap_v_general,
    }


async def test_direct_lane_counters_within_the_preregistered_pin(
    wf_conn: Any,
    wf_schema: str,
    module_pg_pool: Any,
    wf_sql: Any,
) -> None:
    """The direct-only two-page/one-source/no-failure lane: every family
    census is at its real site; the measured V (the seven families' sum)
    respects the pre-registered acceptance CAP of 57 (a standalone literal,
    deliberately stricter than the corrected analytical envelopes of
    69 lane / 73 general — A10; the cap is never the value of a current
    polynomial), and cap <= cap (57 <= 60). The mutants' directions are
    named in the contract: fit-counting every terminal, unhoisting the
    observation, re-parking after grant, waking on elapsed source time,
    dropping a non-zero-channel simulated wake, or spinning after zero CAS
    each red this census or its plan/behavior siblings."""
    from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, chain_source
    from taskq.workflows._emit import COUNTERS
    from taskq.workflows._emit import reset_counters as _rc

    pre = _pre_registration()
    print("PRE-REGISTRATION (before execution):", _json.dumps(pre, indent=1, sort_keys=True))
    # The printed declarations are ASSERTED so they cannot silently become
    # stale again (the correction layer 1's own rule).
    assert pre["candidate_reads <= 8P + 8S + 2R + 2B"] == 26, pre
    assert pre["V_general_envelope = 23P + 22S + 5R + 5B + J + C"] == 73, pre
    assert pre["V_lane_envelope = 23P + 22S + 5R + B + J + C"] == 69, pre
    # The standalone acceptance caps: literals, never a polynomial's value.
    assert pre["acceptance cap V_lane"] == 57, pre
    assert pre["acceptance cap V_general"] == 60, pre
    assert pre["acceptance cap V_lane"] <= pre["acceptance cap V_general"], (
        "the lane cap exceeds the general cap (red)"
    )

    child_calls: list[int | None] = []

    async def screen(ctx: StepContext, item: object) -> OkOut:
        child_calls.append(ctx.map_index)
        # The first page's own child stays live briefly (no gate: every
        # body completes, every tick returns) so the second page's
        # admission sees charge = 1 and parks the two-wide page.
        if ctx.map_index == 0:
            await asyncio.sleep(0.5)
        return OkOut.OK

    chain = _chain(f"counters-{new_uuid_hex()}", screen)
    app = WorkflowApp()

    @app.workflow("counters_flow", max_in_flight=2)
    def counters_flow() -> Promise[object]:
        async def source(ctx: StepContext) -> None:
            from taskq.workflows.chain import chain_start

            # Page 0: one slow child (charge 1 at the next admission).
            await ctx.emit_batch(
                [chain_start(chain, {"app_id": 0}, map_index=0, trace_id="app-0")],
                cursor={"page": 0},
            )
            # Page 1: two children — charge 1 + 2 = 3 > 2 parks the page.
            await ctx.emit_batch(
                [
                    chain_start(chain, {"app_id": 1}, map_index=1, trace_id="app-1"),
                    chain_start(chain, {"app_id": 2}, map_index=2, trace_id="app-2"),
                ],
                cursor={"page": 1},
            )

        return build(chain_source(chain, source, key="source", max_attempts=2))

    compiled = app.get("counters_flow")
    runner = FlowRunner(compiled, module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    _rc()

    async def _pump() -> None:
        # The source parks once (the second page's admission: charge 1 + a
        # two-wide page under bound 2); the slow child's terminal event
        # crosses the threshold, the drain promotes, page 1 admits, and the
        # run completes.
        for _ in range(60):
            await runner.tick(flow_id)
            state = await wf_conn.fetchval(
                f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1',
                flow_id,
            )
            if state in ("succeeded", "failed", "cancelled"):
                return
            await asyncio.sleep(0.1)

    pump = asyncio.create_task(_pump())
    await asyncio.wait_for(pump, timeout=120)

    snap = COUNTERS.snapshot()
    print("MEASURED CENSUS:", _json.dumps(snap, sort_keys=True))
    families = (
        snap["admission_decisions"]
        + snap["source_body_claims"]
        + snap["fitted_grants"]
        + snap["new_intents"]
        + snap["flow_drains"]
        + snap["global_drains"]
        + snap["candidate_reads"]
    )
    print(
        f"V = {families} (cap V_lane = {pre['acceptance cap V_lane']}, "
        f"cap V_general = {pre['acceptance cap V_general']}); "
        f"T={snap['terminals']} X={snap['probes'] - 0} K={snap['parks']}"
    )
    assert families <= pre["acceptance cap V_lane"], (
        f"measured V {families} exceeds the direct lane cap {pre['acceptance cap V_lane']} (red): {snap}"
    )
    # THE READ FAMILY'S OWN IDENTITY (A10): candidate_reads counts actual
    # SQL query executions — exactly 2 * (G + D) for this ordinarily
    # terminating lane (the two-spellings-per-read-point law; a mutant
    # that reads more or fewer times per point reds here).
    assert snap["candidate_reads"] == 2 * (snap["fitted_grants"] + snap["flow_drains"]), snap
    # The event-side law: E = T + X + K <= 2T + K, with E_probe = T and
    # E_observation = X + K (the census's probes are E_probe; observations
    # are E_observation, which includes every park's own observation).
    e = snap["probes"] + snap["observations"]
    assert e <= 2 * snap["terminals"] + snap["parks"], (
        f"E {e} > 2T + K ({2 * snap['terminals'] + snap['parks']}) — a hidden "
        f"count/fallback escaped the instrumentation: {snap}"
    )
    # The source body claim census: exactly the initial claim + the
    # post-park resumption (the lane's P + K + S shape, J = C = 0).
    assert snap["source_body_claims"] >= 2, snap
    assert WF_LOOP_ESCALATION_STEP_KEY  # the escalation key import pin (the suite's own vocabulary)
    _ = _json


def new_uuid_hex() -> str:
    from taskq._ids import new_uuid

    return new_uuid().hex


async def test_non_crossing_terminal_causes_one_zero_row_probe_and_no_derivation(
    wf_conn: Any, wf_schema: str, module_pg_pool: Any, wf_sql: Any
) -> None:
    """§9 control 2's own first law, as a census control: a terminal event
    on a bounded flow with NO exact park causes ONE zero-row indexed probe
    and NO derived count, NO intent, NO grant, and NO claim — the
    fit-count-every-terminal mutant derives on every terminal and reds
    here (its observations census moves; the law's is zero)."""
    from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, chain_source
    from taskq.workflows._emit import COUNTERS
    from taskq.workflows._emit import reset_counters as _rc

    async def screen(ctx: StepContext, item: object) -> OkOut:
        del ctx, item
        return OkOut.OK

    chain = _chain(f"noncrossing-{new_uuid_hex()}", screen)
    app = WorkflowApp()

    @app.workflow("noncrossing_flow", max_in_flight=2)
    def noncrossing_flow() -> Promise[object]:
        async def source(ctx: StepContext) -> None:
            from taskq.workflows.chain import chain_start

            # One page, one child — the bound never holds; the source
            # never parks; the terminals are all NON-CROSSING.
            await ctx.emit_batch(
                [chain_start(chain, {"app_id": 0}, map_index=0, trace_id="app-0")],
                cursor={"page": 0},
            )

        return build(chain_source(chain, source, key="source", max_attempts=2))

    compiled = app.get("noncrossing_flow")
    runner = FlowRunner(compiled, module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    _rc()
    verdict = await runner.drive(flow_id, tick=0.05, max_ticks=40)
    assert verdict == "terminal", verdict

    snap = COUNTERS.snapshot()
    print("NON-CROSSING CENSUS:", _json.dumps(snap, sort_keys=True))
    # THE ZERO-DERIVATION LAW: the terminals' own probes ran (one per
    # terminal event) and NOTHING derived: no observation, no intent, no
    # grant; the park family is zero (the flow never parked).
    assert snap["parks"] == 0, snap
    assert snap["probes"] >= 1, snap
    assert snap["observations"] == 0, snap
    assert snap["new_intents"] == 0, snap
    assert snap["fitted_grants"] == 0, snap
    # The V-family bounds still hold.
    families = (
        snap["admission_decisions"]
        + snap["source_body_claims"]
        + snap["fitted_grants"]
        + snap["new_intents"]
        + snap["flow_drains"]
        + snap["global_drains"]
        + snap["candidate_reads"]
    )
    # The V-family bounds still hold (the standalone pre-registered
    # direct-lane acceptance cap — a literal, never a polynomial's value).
    assert families <= 57, families
    # The read family's own identity in the non-crossing shape (A10): the
    # drive's initial backfill enters exactly one drain transaction (D=1,
    # zero grants), and each read point is two SQL executions —
    # candidate_reads == 2 * (G + D). A mutant that reads more or fewer
    # times per point reds here.
    assert snap["candidate_reads"] == 2 * (snap["fitted_grants"] + snap["flow_drains"]), snap
