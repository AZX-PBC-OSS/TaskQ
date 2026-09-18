"""Differential composition: admission-denied at capacity, then shutdown-interrupted.

A job can meet both of the fleet's non-terminal release paths in one life:
a worker claims it, the admission check answers 429 (no slot — a statement
about capacity, never about the work), the claim's increment is refunded and
``rate_limit_blocked_count`` bumps; a later worker claims it again, takes
SIGTERM mid-flight, and the shutdown release leaves the spent increment
standing and bumps ``interrupt_count``. The denial never ran the actor, so
it may not spend retry budget; the interrupt did start the attempt, so its
increment is exactly what stands (issue #287); and neither may pretend the
other's bookkeeping happened.

The events diet is the second half of the pin. A non-terminal deferral mints
NO per-occurrence rows — sustained saturation must cost one counter bump per
cycle, not a table's worth of history — while the interruption is a real
state transition the fleet must see, so it mints exactly one
``reason='interrupted'`` event. Through the composed chain the row's
durable record is therefore: two counter bumps of different kinds, one
event, no attempt rows, and the attempt counter carrying the interrupted
epoch (the denials refunded theirs; the interrupt does not); identical on
both backends, or the in-memory twin certifies an audit
trail Postgres does not produce.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from taskq.backend._protocol import JobId

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


async def _denied_then_interrupted(side: DiffSide) -> None:
    """Two capacity denials across two claims, then a shutdown release on
    the third — the counters-and-timeline composition under test."""
    enqueued = await side.enqueue("j1", scheduled_in=-1.0)
    jid = JobId(enqueued.id)

    denials: list[str] = []
    for _ in range(2):
        claimed = await side.dispatch("w1", ["default"], limit=5)
        assert claimed == ["j1"], "the scenario requires each claim to land"
        # The consumer's 429 write: admission said no slot, so the claim's
        # increment is refunded and the job reschedules — never charged
        # against the retry budget real executions spend.
        denials.append(await side.mark_snoozed("j1", "w1", 30.0, outcome="rate_limit_denied"))
        # Make the denied row due again without waiting out the deferral,
        # then promote it through the production scheduled→pending sweep —
        # a deferral lands 'scheduled' and only the sweep re-pends it.
        await side.mutate("j1", scheduled_in_s=-1.0)
        side.record("promoted", await side.sweep_promote())
    side.record("denials", denials)

    # The third claim is a different pod's — and that pod takes SIGTERM
    # mid-attempt: the shutdown release counts the interruption and leaves
    # the spent claim standing (the attempt started executing, issue #287).
    claimed = await side.dispatch("w2", ["default"], limit=5)
    assert claimed == ["j1"]
    row = await side.backend.get(jid)
    assert row is not None
    side.record(
        "release",
        await side.backend.mark_interrupted(
            jid, await side.worker("w2"), attempt=row.attempt, hold=timedelta(0)
        ),
    )


async def test_diff_denials_then_shutdown_interrupt_keep_the_counters_straight(
    pg_dsn: str,
) -> None:
    """Denials bump only ``rate_limit_blocked_count``, the interrupt bumps
    only ``interrupt_count``, the denials refund their claim's increment
    while the interrupt leaves its spent epoch standing, and the
    timeline carries exactly the transitions the events diet allows."""
    mem, pg = await run_differential(_denied_then_interrupted, pg_dsn=pg_dsn)
    assert_mirror(
        "two capacity denials followed by a shutdown interruption leave "
        "identical counters, attempt, status and event trail on both "
        "backends",
        mem,
        pg,
    )

    assert pg["records"]["denials"] == ["scheduled", "scheduled"], (
        "each denial reschedules the job (its only terminal exit is its own "
        f"deadline); got {pg['records']['denials']!r}"
    )
    assert pg["records"]["release"] == "pending", (
        "the zero-hold shutdown release lands the row pending at the head "
        f"of the order; got {pg['records']['release']!r}"
    )

    j1 = pg["jobs"]["j1"]
    assert j1["present"] is True
    assert j1["status"] == "pending"
    assert j1["attempt"] == 1, (
        "the two denials refunded their claims (nothing ran), but the "
        "interrupted attempt did start executing: its increment stands and "
        f"the epoch is 1, not 0 (issue #287); got attempt={j1['attempt']!r}"
    )
    assert j1["rate_limit_blocked_count"] == 2, (
        "each 429 counted itself exactly once — never as a snooze, never "
        f"as an interrupt; got {j1['rate_limit_blocked_count']!r}"
    )
    assert j1["snooze_count"] == 0, (
        "a capacity denial is not an actor-requested deferral; snooze_count "
        f"= {j1['snooze_count']!r}"
    )
    assert j1["interrupt_count"] == 1, (
        f"the shutdown release counted exactly itself; got {j1['interrupt_count']!r}"
    )
    assert j1["attempts"] == [], (
        "no attempt ever ran to a reportable outcome — the denials and the "
        f"interruption write no attempt rows; got {j1['attempts']!r}"
    )
    event_kinds = [(e["kind"], e["detail"].get("reason")) for e in j1["events"]]
    assert event_kinds == [("state_change", "interrupted")], (
        "the events diet: the denials mint no per-occurrence rows (the "
        "counters carry them), the interruption mints exactly one; got "
        f"{event_kinds!r}"
    )
