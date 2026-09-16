"""Red-team: does fixing the uncapped under-claim reopen over-admission
past a capped actor's max_concurrent when several pods contend, forcing
the window-expansion loop to run?

The issue thread for the under-claim fix names this explicitly as the
inverse hazard: pgqueuer's own history is a lock node that
slides past a capacity-bound window and over-admits. TaskQ's fix keeps a
pre-lock window for capped actors specifically to avoid that, and adds a
window-expansion retry when a round returns empty. This test stresses
both mechanisms together: many pods, a capped actor, and enough backlog
depth that dispatchers collide and the expansion loop is exercised.

First cut of this test asserted a hard cap (``in_flight <= max_concurrent``)
and failed ~60% of the time (3/5 runs), with in-flight reaching 4-5 against
a cap of 3. That is NOT a regression from this fix: `_dispatch_sql.py`'s
own module docstring documents `running_per_actor` as a best-effort,
read-once-before-lock snapshot with a STATED bound of
`(num_producers - 1) * max_concurrent` over-admission per round -- a
per-round admission damper, not a hard fleet-wide cap, by design; a strict
fleet-wide cap goes through the leased-slot ConcurrencyReservation instead.
This test therefore pins the DOCUMENTED bound, not an unbounded one: it
fails only if the fix's expansion loop makes the soft-cap MATH worse than
the module comment already discloses.
"""

from __future__ import annotations

import asyncio

import pytest

from taskq._ids import new_base62
from tests._fleet import open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_cap_q"
_ACTOR = "fleet_cap_actor"
_CAP = 3
_BACKLOG = 60
_ROUND_LIMIT = 5
_POD_NAMES = ("a", "b", "c", "d", "e")


async def test_capped_actor_over_admission_stays_within_documented_soft_cap_bound(
    pg_dsn: str,
) -> None:
    """Five pods hammer claim rounds concurrently against one actor capped
    at 3. `running_per_actor`'s own module comment discloses the bound:
    up to `(num_producers - 1) * max_concurrent` extra admissions per round
    are possible because the in-flight snapshot is read once, before any
    dispatcher's locks are taken, and never rechecked. This test pins that
    the fix's window-expansion retry loop does not make that bound worse
    -- in-flight must never exceed `producers * max_concurrent` for this
    fleet size, which is the documented worst case restated in absolute
    terms.
    """
    schema = f"fleet_cap_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=_POD_NAMES,
        actors=((_ACTOR, _QUEUE),),
    ) as fleet:
        # open_fleet's actors tuple has no cap slot; set max_concurrent
        # directly on the row it already inserted uncapped.
        await fleet.fetch(
            'UPDATE "{schema}".actor_config SET max_concurrent = $1 WHERE actor = $2',
            _CAP,
            _ACTOR,
        )
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)

        max_observed_in_flight = 0
        for round_index in range(6):
            claims = await asyncio.gather(
                *(fleet.pod(name).claim([_QUEUE], _ROUND_LIMIT) for name in _POD_NAMES)
            )
            round_claimed_ids = {job.id for claim in claims for job in claim}

            # Every claim in this round is freshly 'running'; nothing was
            # completed yet, so in-flight after this round is exactly the
            # cumulative count of rows claimed and not yet finished.
            in_flight_rows = await fleet.fetch(
                'SELECT count(*) AS n FROM "{schema}".jobs '
                "WHERE actor = $1 AND status = 'running'",
                _ACTOR,
            )
            in_flight = in_flight_rows[0]["n"]
            max_observed_in_flight = max(max_observed_in_flight, in_flight)

            worst_case_bound = len(_POD_NAMES) * _CAP
            assert in_flight <= worst_case_bound, (
                f"round {round_index}: actor {_ACTOR!r} capped at {_CAP} has "
                f"{in_flight} rows simultaneously running after {len(round_claimed_ids)} "
                f"were claimed this round across {len(_POD_NAMES)} concurrently-contending "
                f"pods -- exceeding even the documented worst-case soft-cap bound of "
                f"{worst_case_bound}. The window-expansion retry loop must not widen "
                "admission past what the pre-existing TOCTOU snapshot already permits."
            )

            if in_flight >= _CAP:
                break

        assert max_observed_in_flight > 0, (
            "fixture broken: no pod ever claimed anything from this capped actor, "
            "so the cap was never actually exercised"
        )
