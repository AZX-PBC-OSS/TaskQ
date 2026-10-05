"""Override delays are honored EXACTLY — jitter spreads the computed curve only.

#656 taught ``RetryClassifier._retry_decision`` to draw the policy's
multiplicative-symmetric jitter band over an explicit ``RetryOverride.delay``
(a classifier's hint-sourced delay, e.g. a server's ``Retry-After`` value).
Before #656 the override path applied no jitter — the downstream consumer pin
``test_a_429_reprieve_schedules_the_retry_at_the_server_horizon_in_virtual_time``
documents that contract in its own docstring. Drawing over the hint mutates a
value the user's classifier specified: with the default ``jitter=0.2`` a 90s
``Retry-After`` drew from ``[72s, 108s]``, including draws *before* the
server's horizon — the exact bad-citizen behavior the header exists to
prevent, and an explicit direction the default overrode.

The maintainer's law: defaults never get in the way of the user's explicit
direction. An explicit ``RetryOverride.delay`` is an explicit direction, so it
flows through verbatim — only the documented ``max_retry_backoff`` ceiling and
the ``MIN_DEFERRAL_INTERVAL`` floor still bound it. Jitter keeps spreading the
computed curve (that is what jitter is for); ``jitter=0.0`` stays the exact
identity on both paths. A user who wants fleet-spread on a hint applies it in
their own classifier (``apply_jitter``), documented in retries.md §5.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

import taskq.retry as _retry_module
from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, ErrorInfo
from taskq.context import JobContext
from taskq.retry import (
    JobRetryState,
    Retry,
    RetryClassifier,
    RetryOverride,
    RetryPolicy,
    decide_after_failure,
)
from taskq.retry import rate_limit_aware_classifier as _rate_limit_aware_classifier
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)

#: The cennan shape: their classifier hands back the server's 90s reprieve.
_HINT = timedelta(seconds=90)


class _Http429Error(Exception):
    """The user-side 429 carrying the server's ``Retry-After`` header:
    a bare ``.status_code`` (the built-in's claimed-signal duck-type)
    plus ``.headers`` (its sniffed containers)."""

    def __init__(self, retry_after_seconds: int) -> None:
        self.status_code = 429
        self.headers = {"Retry-After": str(retry_after_seconds)}
        super().__init__(f"HTTP 429, Retry-After: {retry_after_seconds}")


def _hint_classifier(exc: BaseException, attempt: int) -> RetryOverride | None:
    """A user classifier that parses the server's hint into an explicit delay."""
    if isinstance(exc, _Http429Error):
        return RetryOverride(
            kind="indefinite",
            delay=timedelta(seconds=int(exc.headers["Retry-After"])),
        )
    return None


def _policy(**overrides: Any) -> RetryPolicy:
    base = RetryPolicy(kind="transient", max_attempts=3)
    return base.model_copy(update=overrides)


def _job_state(*, attempt: int = 1, max_attempts: int = 3) -> JobRetryState:
    return JobRetryState(
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
    )


# ── pin 1: the exact-compliance property (the red-first pin) ────────


@given(seed=st.integers(min_value=0, max_value=2**63 - 1))
def test_explicit_override_delay_is_honored_exactly_under_default_jitter(seed: int) -> None:
    """PROPERTY: an explicit ``RetryOverride.delay`` is honored EXACTLY —
    zero variance over 200 production draws under the default ``jitter=0.2``,
    for every seed. The library never mutates a value the user's classifier
    specified; jitter spreads only the computed curve."""
    _retry_module._production_rng.seed(seed)  # type: ignore[attr-defined]
    policy = _policy()  # default jitter=0.2

    decisions = [
        decide_after_failure(
            StubActorConfig(retry=policy, retry_classifier=_hint_classifier),
            _Http429Error(90),
            _job_state(),
        )
        for _ in range(200)
    ]

    delays = [d.retry_delay for d in decisions if isinstance(d, Retry)]
    assert len(delays) == 200
    assert set(delays) == {_HINT}, (
        f"the override draw mutated the explicit hint (seed={seed}): "
        f"{len(set(delays))} distinct values across 200 draws, expected exactly {_HINT}"
    )


# ── pin 2: the cennan-shaped pin, mirrored upstream ─────────────────


async def test_429_reprieve_schedules_the_retry_exactly_at_the_hint_in_virtual_time() -> None:
    """The downstream consumer's exact scenario: a 429 reprieve + a user
    classifier handing back the server's Retry-After: 90 → the retry is
    scheduled at EXACTLY now + 90s in virtual time — no draw, no band, the
    server's horizon honored to the second."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    backend.register_stub(
        "partner",
        lambda payload, ctx: (_ for _ in ()).throw(_Http429Error(90)),
        retry=_policy(),
        retry_classifier=_hint_classifier,
        payload_type=EmptyPayload,
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="partner",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: test-only access, mirrors test_retry_inmemory.py
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1

    decision = decide_after_failure(
        StubActorConfig(retry=_policy(), retry_classifier=_hint_classifier),
        _Http429Error(90),
        _job_state(),
    )
    assert isinstance(decision, Retry)

    row = await backend.mark_failed_or_retry(
        args.id,
        backend._worker_id,  # type: ignore[reportPrivateUsage]
        ErrorInfo(
            error_class="Http429Error",
            error_message="HTTP 429",
            error_traceback=None,
        ),
        decision.retry_delay,
        attempt=1,
        claim_epoch=1,
    )

    assert row.status == "scheduled"
    assert row.scheduled_at - _START == _HINT, (
        f"the retry was scheduled {row.scheduled_at - _START} after the failure in "
        f"virtual time; the server's exact 90s horizon must be honored verbatim"
    )


# ── pin 2b: the cennan pin's full drain shape, mirrored upstream ────


async def test_a_429_reprieve_schedules_the_retry_at_the_server_horizon_in_virtual_time() -> None:
    """The downstream consumer's pin, mirrored upstream at full fidelity
    (cennan ``tests/unit/test_pipeline_actor_dispatch.py``, same test
    name): the REAL ``rate_limit_aware_classifier`` (the built-in, not a
    stand-in — the original pins its real classifier by identity, this
    pin uses the real built-in for the same reason) turns a 429 carrying
    ``Retry-After: 90`` into a reprieve scheduled at the server's own
    horizon, advanced in VIRTUAL time by a full ``run_until_drained``
    drain — attempt 2 runs and succeeds, the attempt rows' started_at
    gap is exactly the honoured hint to the second (the override path
    applies no jitter), where the static policy would have landed in the
    jittered backoff band.
    """
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    calls: list[int] = []

    def body(payload: object, ctx: JobContext[EmptyPayload]) -> object:
        calls.append(ctx.attempt)
        if len(calls) == 1:
            raise _Http429Error(90)
        # Attempt 2: the provider has recovered.
        return {"ok": True}

    backend.register_stub(
        "partner",
        body,
        retry=_policy(),
        # The REAL built-in, composed as a consumer would: the 429 shape
        # below is the claimed signal (bare .status_code + .headers).
        retry_classifier=_rate_limit_aware_classifier,
        payload_type=EmptyPayload,
    )

    args = EnqueueArgs(
        id=new_job_id(),
        actor="partner",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)

    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None and row.status == "succeeded"
    assert row.attempt == 2, "the reprieve still spends one attempt of the budget (taskq semantics)"
    assert calls == [1, 2], "the actor body ran exactly twice: the reprieved attempt, then success"

    attempt_rows = await backend.get_attempts(args.id)
    assert [a.attempt for a in attempt_rows] == [1, 2]
    gap = attempt_rows[1].started_at - attempt_rows[0].started_at
    assert gap == _HINT, (
        f"the retry must be scheduled at the Retry-After horizon in virtual time, got {gap}; "
        "the policy's jittered backoff answering instead is the #656 defect shape"
    )
    # And the drain genuinely moved the FakeClock to get there: no real second was spent.
    assert clock.now() >= _START + _HINT


# ── pin 3: the curve path STILL jitters (symmetric, unchanged) ──────


def test_curve_derived_delays_still_jitter_symmetrically() -> None:
    """Jitter keeps spreading the computed curve: with the default
    ``jitter=0.2`` the draws over a 5s base straddle the raw value — some
    below, some above — and are not all identical. The curve keeps its
    multiplicative-symmetric band; the override exemption changed nothing
    here."""
    _retry_module._production_rng.seed(20260101)  # type: ignore[attr-defined]
    policy = _policy(base=timedelta(seconds=5))

    decisions = [
        decide_after_failure(
            StubActorConfig(retry=policy),
            RuntimeError("transient"),
            _job_state(),
        )
        for _ in range(200)
    ]

    delays = [d.retry_delay for d in decisions if isinstance(d, Retry)]
    assert len(delays) == 200
    assert len(set(delays)) >= 2, "the curve path must still jitter"
    below = [d for d in delays if d < timedelta(seconds=5)]
    above = [d for d in delays if d > timedelta(seconds=5)]
    assert below, "the symmetric band must still draw below the raw curve value"
    assert above, "the symmetric band must still draw above the raw curve value"


# ── pin 4: jitter=0.0 stays the exact identity on BOTH paths ────────


def test_jitter_zero_is_exact_on_both_paths() -> None:
    """``jitter=0.0`` is the deterministic-suite knob: the hint passes
    through exactly AND the curve lands on its raw value."""
    policy = _policy(jitter=0.0, base=timedelta(seconds=5))

    hint_decision = decide_after_failure(
        StubActorConfig(retry=policy, retry_classifier=_hint_classifier),
        _Http429Error(90),
        _job_state(),
    )
    assert isinstance(hint_decision, Retry)
    assert hint_decision.retry_delay == _HINT

    curve_decision = decide_after_failure(
        StubActorConfig(retry=policy),
        RuntimeError("transient"),
        _job_state(),
    )
    assert isinstance(curve_decision, Retry)
    assert curve_decision.retry_delay == timedelta(seconds=5)


# ── the seam: the classifier surface is unchanged ───────────────────


def test_classify_returns_the_override_delay_verbatim() -> None:
    """The pure seam ``_retry_decision`` sits behind: an override delay
    flows through ``RetryClassifier.classify`` untouched (default jitter),
    only the documented ceiling/floor bounds apply."""
    decision = RetryClassifier.classify(
        policy=_policy(),  # default jitter=0.2
        non_retryable_exceptions=(),
        exception=_Http429Error(90),
        attempt=1,
        override=RetryOverride(kind="indefinite", delay=_HINT),
    )
    assert isinstance(decision, Retry)
    assert decision.retry_delay == _HINT
