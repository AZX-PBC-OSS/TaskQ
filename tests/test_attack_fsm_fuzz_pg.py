"""Twin ≡ PG differential over RANDOM serial operation plans (lane
atk/fsm-properties).

The twin state machine (``test_attack_fsm_fuzz.py``) fuzzes the mirror in
isolation; this module takes the SERIAL-ORDER subset of the same operation
alphabet - one queue, fixed tokens, no concurrency - draws random plans
with Hypothesis, and drives the SAME plan through both backends via the
:class:`tests.test_rt_diff_harness.DiffSide` adapter.  Postgres is the
contract source: ``assert_mirror`` compares every observable token-for-
token, and each side's snapshot is additionally checked against the same
invariants the twin machine pins (conservation, truthful terminal columns,
legal event edges, one attempt row per epoch, fence no-ops on stale
views).

Token-limited by design (``max_examples=10``): each example pays a fresh
PG schema + migrations, so the depth lives in the twin machine while this
suite certifies that the drawn plans behave IDENTICALLY on production
Postgres.
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq.exceptions import WorkerOwnershipMismatch
from tests.test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration

# ── The drawn plan alphabet ────────────────────────────────────────────
# Fixed tokens keep the two sides' observables comparable token-for-token;
# ops on tokens that hold no row (a draw before its enqueue) record
# "<absent>" and skip - the SAME skip on both sides is itself an observable.

_TOKENS = ("j1", "j2", "j3")
_WORKERS = ("w1", "w2")

Op = tuple[Any, ...]

_FSMS = st.one_of(
    st.tuples(st.just("enqueue"), st.sampled_from(_TOKENS)),
    st.tuples(st.just("dispatch"), st.sampled_from(_WORKERS), st.integers(1, 2)),
    st.tuples(st.just("succeed"), st.sampled_from(_TOKENS), st.sampled_from(_WORKERS)),
    st.tuples(
        st.just("fail"),
        st.sampled_from(_TOKENS),
        st.sampled_from(_WORKERS),
        st.sampled_from([None, 0.0, 30.0]),
    ),
    st.tuples(
        st.just("retry_after"),
        st.sampled_from(_TOKENS),
        st.sampled_from(_WORKERS),
        st.sampled_from([0.0, 30.0]),
        st.booleans(),
    ),
    st.tuples(
        st.just("snooze"),
        st.sampled_from(_TOKENS),
        st.sampled_from(_WORKERS),
        st.sampled_from(["snoozed", "reservation_denied", "rate_limit_denied"]),
    ),
    st.tuples(st.just("cancel_request"), st.sampled_from(_TOKENS)),
    st.tuples(st.just("worker_cancel"), st.sampled_from(_TOKENS), st.sampled_from(_WORKERS)),
    st.tuples(st.just("retry_job"), st.sampled_from(_TOKENS)),
    st.tuples(st.just("expire_lock"), st.sampled_from(_TOKENS)),
    st.just(("sweep_reclaim",)),
    st.just(("sweep_deadline",)),
    st.just(("sweep_promote",)),
)


def _has_token(side: DiffSide, token: str) -> bool:
    return token in side._jobs_by_token  # pyright: ignore[reportPrivateUsage]  # Why: the plan executor needs the adapter's registry; the established same-module pattern.


async def _exec_op(side: DiffSide, op: Op) -> Any:
    """Execute one drawn op through the adapter; return a comparable token."""
    kind = op[0]
    if kind == "enqueue":
        token = str(op[1])
        row = await side.enqueue(token, retry_jitter=0.0)
        return ("enqueued", token, row.status)
    if kind == "sweep_reclaim":
        return ("sweep_reclaim", await side.sweep_reclaim())
    if kind == "sweep_deadline":
        return ("sweep_deadline", await side.sweep_deadline())
    if kind == "sweep_promote":
        return ("sweep_promote", await side.sweep_promote())
    if len(op) < 2 or not _has_token(side, str(op[1])):
        return ("<absent>", kind)
    token = str(op[1])
    if kind == "dispatch":
        rows = await side.dispatch(str(op[1]), ["default"], int(op[2]))
        return ("dispatched", str(op[1]), rows)
    if kind == "succeed":
        return ("succeed", token, await side.mark_succeeded(token, str(op[2])))
    if kind == "fail":
        delay = op[3]
        try:
            row = await side.mark_failed_or_retry(token, str(op[2]), retry_delay_s=delay)
        except WorkerOwnershipMismatch:
            # The documented fenced-out signal (stale epoch, wrong worker,
            # or a row that moved): the typed error IS the observable.
            return ("fail", token, "mismatch")
        return ("fail", token, row.status)
    if kind == "retry_after":
        return (
            "retry_after",
            token,
            await side.mark_retry_after(
                token, str(op[2]), float(op[3]), consume_budget=bool(op[4])
            ),
        )
    if kind == "snooze":
        return (
            "snooze",
            token,
            await side.mark_snoozed(token, str(op[2]), 30.0, outcome=op[3]),
        )
    if kind == "cancel_request":
        return ("cancel_request", token, await side.write_cancel_request(token, "operator"))
    if kind == "worker_cancel":
        return ("worker_cancel", token, await side.mark_cancelled(token, str(op[2])))
    if kind == "retry_job":
        return ("retry_job", token, await side.retry_job(token))
    if kind == "expire_lock":
        await side.mutate(token, lock_expired_ago_s=1.0)
        return ("expire_lock", token)
    raise AssertionError(f"unreachable op kind: {kind}")


def _plan_scenario(plan: list[Op]) -> Any:
    async def scenario(side: DiffSide) -> None:
        outcomes = [await _exec_op(side, op) for op in plan]
        side.record("outcomes", outcomes)

    return scenario


def _assert_snapshot_invariants(obs: dict[str, Any]) -> None:
    """The twin machine's invariants, projected onto a snapshot."""
    from taskq.backend.statemachine import TERMINAL_STATUSES, VALID_TRANSITIONS

    terminal = TERMINAL_STATUSES
    for token, job in obs["jobs"].items():
        if not job.get("present", False):
            continue
        # Truthful terminal columns.
        assert (job["finished_at"] is not None) == (job["status"] in terminal), (
            f"{token}: untruthful terminal columns"
        )
        # One attempt row per epoch.
        seen: set[int] = set()
        for a in job["attempts"]:
            assert a["attempt"] not in seen, f"{token}: duplicate attempt epoch {a['attempt']}"
            seen.add(a["attempt"])
        # Legal event edges; terminal absorbing.
        terminal_seen = False
        for e in job["events"]:
            if e["kind"] != "state_change":
                continue
            frm = e["detail"]["from_state"]
            to = e["detail"]["to_state"]
            assert to in VALID_TRANSITIONS[frm], f"{token}: illegal event edge {frm}->{to}"
            assert not terminal_seen, f"{token}: state_change after a terminal event"
            if to in terminal:
                terminal_seen = True


@settings(max_examples=10, deadline=None)
@given(plan=st.lists(_FSMS, max_size=24))
async def test_diff_fsm_serial_plans_mirror_pg(plan: list[Op], pg_dsn: str) -> None:
    """A random serial op-plan lands IDENTICALLY on both backends, and each
    side's final snapshot satisfies the state-machine invariants."""
    mem_obs, pg_obs = await run_differential(_plan_scenario(plan), pg_dsn=pg_dsn)
    _assert_snapshot_invariants(mem_obs)
    _assert_snapshot_invariants(pg_obs)
    assert_mirror(
        "a random serial operation plan (enqueue/dispatch/terminal/deferral/"
        "cancel/sweeps/lease-expiry) leaves every observable - statuses, "
        "attempts, event trails, recorded step outcomes - identical on both "
        "backends",
        mem_obs,
        pg_obs,
    )


async def _ceiling_repeat_scenario(side: DiffSide) -> None:
    """The smallint ceiling's claim-clamped repeat epoch, serially.

    An ``indefinite`` row reclaimed at ``attempt=32767`` is claimed twice at
    the SAME (clamped) attempt number - the documented repeat (the dispatch
    claim's ``LEAST(j.attempt + 1, 32767)`` saturation comment).  Between
    the two claims the row defers with a CONSUMING RetryAfter (an
    indefinite row never exhausts, so the re-pend arm owns it), which
    writes the epoch's attempt row; the second claim's terminal write then
    hits the same ``(job_id, attempt)`` key.  PG's doctrine for that key
    collision is spelled on every attempt INSERT: ``ON CONFLICT DO NOTHING``
    - "keep the first record, never roll the transition back".  The mirror
    must keep exactly the same one row.
    """
    await side.plant(
        "j1",
        status="running",
        worker_token="w1",
        attempt=32767,
        max_attempts=1,
        retry_kind="indefinite",
        lock_expired_ago_s=1.0,
    )
    await side.sweep_reclaim()
    # The reclaim re-queues at the row's own crash backoff; pull the
    # re-scheduled instant into each side's past so the claim can take it.
    await side.mutate("j1", scheduled_in_s=-1.0)
    first = await side.dispatch("w1", ["default"], 5)
    side.record("first_claim", first)
    res = await side.mark_retry_after("j1", "w1", 0.0, consume_budget=True)
    side.record("retry_after_res", res)
    row = await side.backend.get(side._jobs_by_token["j1"])  # pyright: ignore[reportPrivateUsage]  # Why: the ceiling probe needs the deferral's landing status; the same-module pattern.
    side.record("after_defer_status", None if row is None else row.status)
    side.record("after_defer_attempt", None if row is None else row.attempt)
    second = await side.dispatch("w1", ["default"], 5)
    side.record("second_claim", second)
    landed = await side.mark_succeeded("j1", "w1")
    side.record("succeed_res", landed)
    final = await side.backend.get(side._jobs_by_token["j1"])  # pyright: ignore[reportPrivateUsage]  # Why: same pattern.
    attempts = await side.backend.get_attempts(side._jobs_by_token["j1"])  # pyright: ignore[reportPrivateUsage]  # Why: same pattern.
    side.record(
        "final",
        {
            "status": None if final is None else final.status,
            "attempt_outcomes": [a.outcome for a in attempts],
        },
    )


async def test_diff_claim_clamped_ceiling_keeps_first_attempt_row(pg_dsn: str) -> None:
    """FENCE/EPOCH: at the smallint ceiling a clamped repeat claim re-uses
    the attempt number; the attempt ledger keeps the FIRST record for that
    ``(job_id, attempt)`` key on BOTH backends (PG's ON CONFLICT DO NOTHING
    doctrine, quoted on every attempt INSERT)."""
    mem, pg = await run_differential(_ceiling_repeat_scenario, pg_dsn=pg_dsn)
    assert pg["records"]["first_claim"] == ["j1"]
    assert pg["records"]["after_defer_status"] == "pending"
    assert pg["records"]["second_claim"] == ["j1"]
    assert pg["records"]["succeed_res"] is True
    assert pg["records"]["final"]["status"] == "succeeded"
    assert_mirror(
        "at the smallint ceiling, a claim-clamped repeat of attempt 32767 "
        "records the epoch's attempt row ONCE: the first record (the "
        "consuming RetryAfter's 'snoozed') survives and the second claim's "
        "terminal write does not add a second row for the same "
        "(job_id, attempt) key - identically on both backends",
        mem,
        pg,
    )
