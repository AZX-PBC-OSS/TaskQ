"""strict_fifo's cross-actor rotation is a provable bound, not a hand-wave.

docs/guides/workers.md:214 currently reads: "``strict_fifo`` (default) |
Jobs are dispatched in priority-then-time order (``priority DESC,
scheduled_at, id``). Every pending job competes freely — a deep queue of
one actor can starve others if all candidates share high priority." Read
plainly, that sentence tells an adopter coming from Celery/Oban/Sidekiq
that ``strict_fifo`` offers NO cross-actor fairness guarantee at all, and
that they must reach for ``round_robin`` + ``fairness_key`` the moment more
than one actor shares a queue.

That is not what the dispatch SQL actually does. The claim statement's
``stamp`` CTE writes ``actor_config.last_claimed_at`` for every actor
admitted in a round, and the cross-round tiebreak (after ``pending_rank``
and ``priority``) is ``actor_claimed_at ASC NULLS FIRST`` — never-claimed
actors first, then least-recently-claimed
(src/taskq/backend/_dispatch_sql.py lines 78-95, 460-471, 505-513). This
rotation applies in BOTH dispatch modes; nothing in its implementation is
conditional on ``queues.mode``. The module docstring even states the
provable shape explicitly (lines 78-95): "a stable total order that
re-elects the same prefix of actors every round once more actors hold due
work than the round's limit admits, starving the rest silently... Priority
still dominates the stamp, so the operator's priority bias keeps its
meaning; the stamp removes only the accidental starvation among
equal-priority peers."

If that stamp is a true round-robin partition (each round admits a
disjoint, least-recently-served cohort), the bound is TIGHT and provable:
with ``actor_count`` equal-priority actors sharing a queue and a round
``limit``, every actor must be first served within
``ceil(actor_count / limit)`` rounds — not merely "eventually," and not
only under generous slack. A single-connection probe against the raw SQL
(tests/test_dispatch_cohort_rotation_tight_bound_scratch.py, pre-existing
scratch work in this tree) already confirms the tight bound holds for both
``strict_fifo`` and ``round_robin`` at the SQL layer. This test confirms
the SAME tight bound holds through the production fleet path — real
``Pod.claim()`` calls (``backend.dispatch_batch``), real Postgres, the
harness in ``tests/_fleet.py`` — so the contract is pinned at the layer an
adopter actually calls, not only at the SQL layer underneath it.

Vendor framing: this is the guarantee Sidekiq's ``:strict`` queue mode
explicitly disclaims providing across DIFFERENT queues (a strict-ordered
queue can starve a lower one — vendor/sidekiq/lib/sidekiq/capsule.rb:57-58,
the ``:strict`` comment: "all queues have 0 weight and are checked strictly
in order") and that Sidekiq's default weighted-random mode only
approximates probabilistically, never with a bound
(vendor/sidekiq/lib/sidekiq/fetch.rb — queue order is re-shuffled by
weight, not rotated by a provable schedule). TaskQ's actor-level rotation
is stronger than either Sidekiq mode: it is a hard, provable, per-round
bound, not a probabilistic approximation or a documented-as-unbounded
"strict" order. The finding here is not that TaskQ's engineering is
lacking — it demonstrably is not — but that workers.md:214 describes the
weaker (Sidekiq-strict-like) behaviour TaskQ does NOT have, instead of the
stronger, bounded behaviour it does. This test exists so that line can be
rewritten against a machine-checked number instead of prose that
undersells its own guarantee.

This test currently PASSES — per the sweep's own rule ("if your test
passes, that is coverage worth keeping — also leave it on disk"), it stays
in the suite as the pinned, provable contract the doc should be rewritten
to state plainly: every actor sharing a strict_fifo queue is served within
ceil(actor_count / round_limit) rounds, full stop, no fairness_key or
round_robin required.
"""

from __future__ import annotations

import math

import pytest

from taskq._ids import new_base62
from tests._fleet import Fleet, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_strict_fifo_bound_q"
_ACTOR_COUNT = 24
_ROUND_LIMIT = 5
_DEPTH_PER_ACTOR = 8  # each actor holds far more than one round's worth
_TIGHT_BOUND = math.ceil(_ACTOR_COUNT / _ROUND_LIMIT)


def _actor_names() -> list[str]:
    return [f"strict_bound_{i:02d}" for i in range(_ACTOR_COUNT)]


async def _first_round_served(fleet: Fleet, rounds: int) -> dict[str, int]:
    """Map actor -> the 1-indexed round it was FIRST claimed in, via the
    real fleet claim path (Pod.claim -> backend.dispatch_batch), not raw
    SQL. Stops early once every actor has been served at least once.
    """
    actors = set(_actor_names())
    first_served: dict[str, int] = {}
    for round_no in range(1, rounds + 1):
        claimed = await fleet.pod("pod-1").claim([_QUEUE], _ROUND_LIMIT)
        if not claimed:
            break
        for job in claimed:
            first_served.setdefault(job.actor, round_no)
        if actors <= first_served.keys():
            break
    return first_served


async def test_strict_fifo_serves_every_actor_within_the_provable_round_bound(
    pg_dsn: str,
) -> None:
    """Every equal-priority actor on a strict_fifo queue is first served
    within ceil(actor_count / round_limit) rounds through the real fleet
    claim path — the tight bound, not merely "eventually" within a
    generous multiple of it.

    Contrast with tests/test_fleet_fairness_starvation.py's existing
    ``test_every_actor_is_dispatched_within_a_bounded_number_of_rounds``,
    which asserts the same property with 4x slack (``_ROUNDS = (actor_count
    * 4 + limit - 1) // limit``) — enough to catch outright starvation but
    not tight enough to state the actual guarantee an adopter can rely on.
    This test asserts the number the rotation stamp's own design promises.
    """
    actors = _actor_names()
    schema = f"fleet_strict_bound_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=tuple((actor, _QUEUE) for actor in actors),
    ) as fleet:
        # queue has no row -> defaults to strict_fifo (docs/guides/workers.md:233).
        for actor in actors:
            await fleet.enqueue(_DEPTH_PER_ACTOR, actor=actor, queue=_QUEUE)

        # Slack ceiling only to detect outright starvation distinctly from
        # a loose bound; the real assertion below is against _TIGHT_BOUND.
        first_served = await _first_round_served(fleet, _TIGHT_BOUND * 2)

        never = sorted(set(actors) - first_served.keys())
        max_round = max(first_served.values()) if first_served else None

        assert not never, (
            f"{len(never)} of {_ACTOR_COUNT} actors were never claimed within "
            f"{_TIGHT_BOUND * 2} rounds (2x the tight bound {_TIGHT_BOUND}) through the "
            f"real fleet claim path: {never}. This is outright starvation, not merely a "
            "loose bound — worse than the doc's own worst-case framing."
        )
        assert max_round is not None and max_round <= _TIGHT_BOUND, (
            f"strict_fifo's rotation bound is NOT tight through the fleet claim path: the "
            f"last actor was first served in round {max_round}, exceeding the provable "
            f"ceil(actor_count/round_limit) = {_TIGHT_BOUND}. Every actor was eventually "
            f"served, but the rotation stamp's own design (src/taskq/backend/_dispatch_sql.py "
            "lines 78-95) promises a tight per-round partition — if this fails, the "
            "guarantee that promise describes does not hold at the production dispatch "
            "path, and docs/guides/workers.md:214's cautious framing would be closer to "
            "correct than the stronger claim this test set out to pin."
        )
