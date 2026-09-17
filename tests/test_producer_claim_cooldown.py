"""The producer coalesces bursts of claim triggers instead of claiming per trigger.

A wake NOTIFY is schema-wide and every completion frees a slot, so without
a floor between rounds a burst of either drives one full dispatch round —
the most expensive statement in the system, plus window expansions for
every loser — per trigger. After a round that came back short (fewer rows
than asked, or none) the producer waits a small jittered cooldown before
the next round and folds every trigger that lands meanwhile into that one
round. A full round
keeps re-claiming immediately (there is backlog to drain), a trigger that
lands mid-round is never lost, and the fallback poll keeps its own cadence.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import random
import time
from types import SimpleNamespace
from typing import cast

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobRow
from taskq.testing.assertions import wait_for_condition
from taskq.testing.jobs import make_job_row
from taskq.worker.run import (
    _CLAIM_COOLDOWN_SECONDS,
    _POLL_JITTER_FRACTION,
    producer_loop,
)

_COOLDOWN_MIN_S = _CLAIM_COOLDOWN_SECONDS * (1.0 - _POLL_JITTER_FRACTION)
_SETTLE_S = 0.4
"""Long enough for any trailing coalesced round to land after a burst."""


class _WakeBackend:
    """dispatch_batch that records round start times and answers from a
    script; ``subscribe_wake`` hands the test the producer's wake event."""

    def __init__(self, script: list[list[JobRow]] | None = None) -> None:
        self.script = script if script is not None else []
        self.rounds: list[tuple[float, int]] = []
        self.wake = asyncio.Event()
        self.round_gate: asyncio.Event | None = None

    def subscribe_wake(self) -> _WakeSubscription:
        return _WakeSubscription(self.wake)

    async def dispatch_batch(
        self, *, worker_id: object, queues: object, limit: int, lock_lease: object
    ) -> list[JobRow]:
        self.rounds.append((time.monotonic(), limit))
        if self.round_gate is not None:
            gate, self.round_gate = self.round_gate, None
            await gate.wait()
        if self.script:
            return self.script.pop(0)[:limit]
        return []


class _WakeSubscription:
    def __init__(self, wake: asyncio.Event) -> None:
        self._wake = wake

    async def __aenter__(self) -> asyncio.Event:
        return self._wake

    async def __aexit__(self, *args: object) -> None:
        return None


def _deps(*, maxsize: int, notify_enabled: bool) -> SimpleNamespace:
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=notify_enabled,
        poll_interval=5.0,
        notify_poll_interval=5.0,
        max_concurrency=maxsize,
    )
    return SimpleNamespace(
        settings=settings,
        liveness=SimpleNamespace(tick=lambda *a, **k: None, forget=lambda *a, **k: None),
        disowned_jobs=set(),
    )


class _Producer:
    def __init__(
        self,
        backend: _WakeBackend,
        *,
        maxsize: int,
        notify_enabled: bool = True,
        slot_freed: asyncio.Event | None = None,
    ) -> None:
        self.backend = backend
        self.local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=maxsize)
        self.shutdown_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self._notify_enabled = notify_enabled
        self._maxsize = maxsize
        self._slot_freed = slot_freed

    async def __aenter__(self) -> _Producer:
        self.task = asyncio.create_task(
            producer_loop(
                _deps(maxsize=self._maxsize, notify_enabled=self._notify_enabled),  # type: ignore[arg-type]  # Why: the established producer-loop unit pattern — a namespace with the fields the loop reads.
                self.local_queue,
                self.shutdown_event,
                self.stop_event,
                backend=cast(Backend, self.backend),
                worker_id=new_uuid(),
                slot_freed_event=self._slot_freed,
                rng=random.Random(7),  # noqa: S311  # Why: a fixed seed keeps the cooldown jitter deterministic; nothing cryptographic.
            )
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.shutdown_event.set()
        self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.task


def _coalesced_round_bound(burst_elapsed_s: float) -> int:
    """The most rounds a burst may cost: one on its leading edge, at most one
    per cooldown while it lasts, and one trailing round for whatever landed
    during the final cooldown."""
    return 1 + math.ceil(burst_elapsed_s / _COOLDOWN_MIN_S) + 1


async def test_a_wake_storm_collapses_into_a_few_claim_rounds() -> None:
    """Twenty NOTIFY wakes a couple of milliseconds apart against an empty
    backlog cost at most a handful of rounds, not twenty."""
    backend = _WakeBackend()
    async with _Producer(backend, maxsize=4):
        await wait_for_condition(
            lambda: len(backend.rounds) == 1, description="the initial empty round"
        )
        rounds_before = len(backend.rounds)

        burst_started = time.monotonic()
        for _ in range(20):
            backend.wake.set()
            await asyncio.sleep(0.002)
        burst_elapsed = time.monotonic() - burst_started
        await asyncio.sleep(_SETTLE_S)

        rounds_after_burst = len(backend.rounds) - rounds_before
        assert rounds_after_burst <= _coalesced_round_bound(burst_elapsed), (
            f"{rounds_after_burst} claim rounds for 20 wakes over {burst_elapsed * 1000:.0f} ms "
            "— every NOTIFY still drives its own dispatch round; the wake storm is "
            "not coalesced"
        )
        assert rounds_after_burst >= 1


async def test_a_wake_during_a_round_yields_exactly_one_follow_up_round() -> None:
    """A NOTIFY that lands while a round is in flight may announce a row the
    round's snapshot predates: it must produce one follow-up round — not
    zero (lost) and not one per NOTIFY (duplicated)."""
    backend = _WakeBackend()
    gate = asyncio.Event()
    backend.round_gate = gate
    async with _Producer(backend, maxsize=4):
        await wait_for_condition(
            lambda: len(backend.rounds) == 1, description="the first round to start"
        )
        backend.wake.set()
        await asyncio.sleep(0.005)
        backend.wake.set()
        gate.set()
        await asyncio.sleep(_SETTLE_S)

        assert len(backend.rounds) == 2, (
            f"{len(backend.rounds)} rounds: a wake that landed mid-round must cost "
            "exactly one follow-up round"
        )


async def test_a_short_round_waits_out_the_cooldown_before_the_next() -> None:
    """A round that returns fewer rows than it asked for is the signal the
    backlog is (nearly) drained; the next round starts no sooner than the
    cooldown after it — a re-claim on its heels would only pay for window
    expansions."""
    backend = _WakeBackend(script=[[make_job_row(status="pending")]])
    async with _Producer(backend, maxsize=4):
        await wait_for_condition(
            lambda: len(backend.rounds) >= 2, description="the round after the short one"
        )
        (first_started, first_limit), (second_started, _) = backend.rounds[:2]

    assert first_limit == 4
    assert second_started - first_started >= _COOLDOWN_MIN_S, (
        f"the round after a short one started {(second_started - first_started) * 1000:.1f} ms "
        "later — inside the cooldown"
    )


async def test_a_full_round_re_claims_without_a_cooldown() -> None:
    """A round that filled every slot it asked for says there is backlog to
    drain: the producer keeps claiming as slots free, with no floor."""
    backend = _WakeBackend(script=[[make_job_row(status="pending")]])
    slot_freed = asyncio.Event()
    async with _Producer(backend, maxsize=1, notify_enabled=False, slot_freed=slot_freed) as p:
        await wait_for_condition(
            lambda: len(backend.rounds) == 1, description="the first (full) round"
        )
        await p.local_queue.get()
        freed_at = time.monotonic()
        slot_freed.set()
        await wait_for_condition(
            lambda: len(backend.rounds) == 2, description="the re-claim after the freed slot"
        )
        second_started, _ = backend.rounds[1]

    assert second_started - freed_at < _COOLDOWN_MIN_S, (
        "a full round was followed by a cooldown — backlog drain must stay immediate"
    )


async def test_a_poll_timed_round_owes_no_cooldown(monkeypatch: object) -> None:
    """The fallback poll is its own jittered cadence: the round it times
    never adds the cooldown on top, or an idle poll-only worker would
    claim at poll + cooldown instead of the interval the operator set."""
    import pytest

    assert isinstance(monkeypatch, pytest.MonkeyPatch)
    poll_interval = 0.2
    backend = _WakeBackend()
    stop_event = asyncio.Event()
    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def _recording_sleep(delay: float, result: object = None) -> object:
        # Signature mirrors asyncio.sleep(delay, result); records and yields
        # once without waiting, so the producer cycles through many polls
        # and every timer wait it takes — poll or cooldown — lands here.
        sleeps.append(delay)
        await real_sleep(0)
        if len(sleeps) >= 6:
            stop_event.set()
        return result

    monkeypatch.setattr(asyncio, "sleep", _recording_sleep)
    deps = _deps(maxsize=4, notify_enabled=False)
    deps.settings.poll_interval = poll_interval
    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=4)

    await asyncio.wait_for(
        producer_loop(
            deps,  # type: ignore[arg-type]  # Why: the established producer-loop unit pattern.
            local_queue,
            asyncio.Event(),
            stop_event,
            backend=cast(Backend, backend),
            worker_id=new_uuid(),
            rng=random.Random(11),  # noqa: S311  # Why: a fixed seed keeps the jitter deterministic; nothing cryptographic.
        ),
        timeout=5.0,
    )

    poll_floor = poll_interval * (1.0 - _POLL_JITTER_FRACTION) - 1e-9
    assert len(sleeps) >= 6
    assert all(d >= poll_floor for d in sleeps), (
        f"a cooldown-sized wait {[d for d in sleeps if d < poll_floor]} was added to "
        "poll-timed rounds"
    )
