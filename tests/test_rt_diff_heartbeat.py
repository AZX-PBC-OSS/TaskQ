"""Differential attack on the heartbeat renewal's lease bookkeeping.

The claim stamps ``started_at``, ``last_heartbeat_at`` and
``lock_expires_at`` from one clock reading, and the renewal restamps the
pair from the beat's own reading. The lease's observable is therefore the
DURATION ``lock_expires_at - last_heartbeat_at``: a renewal that moved the
heartbeat stamp without moving the lease (a no-op renewal behind a mask of
moving stamps) reads a duration short by the elapsed between the claim and
the beat, and the differential sees the divergence at any logical offset.
"""

from __future__ import annotations

import pytest

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


async def _heartbeat_renewal_holds_the_lease(side: DiffSide) -> None:
    await side.enqueue("b1", scheduled_in=-1.0)
    await side.dispatch("w1", ["default"], limit=1)
    # The beat lands at a known logical offset from the claim: the mirror's
    # clock jumps 2s, PG's wall advances 2s.
    await side.advance_clock(2.0)
    side.record("renewed", await side.heartbeat("w1", lock_lease_s=60.0))


async def test_diff_heartbeat_renewal_holds_the_lease(pg_dsn: str) -> None:
    """The renewal restamps the running row's lease from the beat's own
    instant: ``lock_expires_at`` reads the lease's duration above
    ``last_heartbeat_at`` on both backends, and the beat's own age above
    the claim reads the elapsed - either half lagging the other (a renewal
    that moved one stamp and not both) diverges on the pair."""
    mem, pg = await run_differential(_heartbeat_renewal_holds_the_lease, pg_dsn=pg_dsn)
    assert_mirror(
        "the heartbeat renewal restamps the running row's lease from the "
        "beat's own clock reading: lock_expires_at reads the lease's own "
        "duration above last_heartbeat_at, and the beat's age above the "
        "claim reads the elapsed, on both backends - a renewal that moved "
        "one stamp without the other reads wrong on one of the two and "
        "diverges",
        mem,
        pg,
    )
    assert pg["records"]["renewed"] == 1
    assert pg["jobs"]["b1"]["lock_expires_at"] == 60
    assert pg["jobs"]["b1"]["heartbeat_age"] == 2
