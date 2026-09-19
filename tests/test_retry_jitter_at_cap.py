"""Jitter survives the backoff cap: a capped cohort is spread, not stacked.

The backoff curve saturates at the cap - the default exponential policy
from attempt 11 on, every ``fixed``/``linear`` policy whose base meets the
cap, every ``indefinite`` job, every reclaimed cohort at the ceiling, and
the operator's ``max_retry_backoff``. Jitter used to be applied to the
saturated value and the result clipped at the cap, so the upper half of the
band collapsed onto ``cap`` exactly and roughly half of any capped cohort
came due at the same instant - the thundering herd jitter exists to
prevent, on precisely the retries most likely to be a fleet-wide event.

The cap is now a bound on the jitter BAND, not on the drawn value: the band
``[raw·(1-j), raw·(1+j)]`` is clipped to the cap before the draw, so a
saturated row draws uniformly over ``[cap·(1-j), cap]``. The documented
bounds (``0 ≤ delay ≤ cap``) are unchanged; ``jitter=0`` stays the identity.
"""

import random
from datetime import timedelta
from uuid import UUID

from taskq.retry import RetryPolicy, _compute_reclaim_backoff, compute_backoff

_CAP = timedelta(hours=1)
_JITTER = 0.2
_DRAWS = 10_000
#: The share of a capped cohort allowed to land on exactly the cap. A
#: continuous draw over ``[cap·(1-j), cap]`` produces essentially none;
#: the clipped formula produced ~50 %.
_MAX_SHARE_AT_CAP = 0.02


def _capped_policy(**overrides: object) -> RetryPolicy:
    base = RetryPolicy(backoff="exponential", base=timedelta(seconds=5), cap=_CAP, jitter=_JITTER)
    return base.model_copy(update=overrides)


def test_a_capped_exponential_cohort_does_not_pile_onto_the_cap() -> None:
    policy = _capped_policy()
    rng = random.Random(7)
    # Attempt 20: 5 s · 2^19 is far past the one-hour cap.
    delays = [compute_backoff(policy, attempt=20, rng=rng) for _ in range(_DRAWS)]

    at_cap = sum(1 for d in delays if d == _CAP)
    assert at_cap / _DRAWS <= _MAX_SHARE_AT_CAP, (
        f"{at_cap} of {_DRAWS} capped draws landed on exactly the cap: the jitter "
        "band was clipped instead of being fitted under the cap"
    )
    assert max(delays) <= _CAP
    assert min(delays) >= _CAP * (1 - _JITTER)
    # The draw is spread over the band, not bunched at either edge.
    below_midpoint = sum(1 for d in delays if d < _CAP * (1 - _JITTER / 2))
    assert 0.35 < below_midpoint / _DRAWS < 0.65


def test_a_fixed_policy_at_the_operator_ceiling_is_spread_under_it() -> None:
    """``backoff='fixed'`` with base above the ceiling: every raw value
    exceeds the effective cap, the shape the operator ceiling exists for."""
    policy = RetryPolicy(backoff="fixed", base=timedelta(days=3), cap=timedelta(days=7), jitter=0.2)
    ceiling = timedelta(hours=24)
    rng = random.Random(11)
    delays = [
        compute_backoff(policy, attempt=1, rng=rng, max_retry_backoff=ceiling)
        for _ in range(_DRAWS)
    ]
    at_cap = sum(1 for d in delays if d == ceiling)
    assert at_cap / _DRAWS <= _MAX_SHARE_AT_CAP
    assert ceiling * 0.8 <= min(delays) <= max(delays) <= ceiling


def test_a_reclaimed_capped_cohort_is_spread_by_its_deterministic_fraction() -> None:
    """The reclaim twin draws its fraction from the row identity; over a
    cohort of rows it must spread the same way the RNG path does."""
    policy = _capped_policy()
    delays = [
        _compute_reclaim_backoff(policy, 20, job_id=UUID(int=i)) for i in range(1, _DRAWS + 1)
    ]
    at_cap = sum(1 for d in delays if d == _CAP)
    assert at_cap / _DRAWS <= _MAX_SHARE_AT_CAP
    assert max(delays) <= _CAP
    assert min(delays) >= _CAP * (1 - _JITTER)


def test_below_the_cap_the_symmetric_band_is_unchanged() -> None:
    """A raw value whose whole band fits under the cap keeps the documented
    ``raw·U(1-j, 1+j)`` spread."""
    policy = _capped_policy()
    rng = random.Random(3)
    delays = [compute_backoff(policy, attempt=1, rng=rng) for _ in range(_DRAWS)]
    assert timedelta(seconds=4) <= min(delays)
    assert max(delays) <= timedelta(seconds=6)
    assert max(delays) > timedelta(seconds=5.5)
    assert min(delays) < timedelta(seconds=4.5)


def test_jitter_zero_at_the_cap_is_exactly_the_cap() -> None:
    policy = _capped_policy(jitter=0.0)
    assert compute_backoff(policy, attempt=20) == _CAP
    assert _compute_reclaim_backoff(policy, 20, job_id=UUID(int=1)) == _CAP
