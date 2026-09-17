"""The rebuild loop behind a ReloadSchedule (taskq._reload_loop).

Driven with a fake clock: the loop's only contact with time is the
``sleep`` seam, so a recording sleeper pins *when* a rebuild is scheduled
relative to the lease the factory was granted, not merely that one
eventually happens.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import structlog.testing

from taskq._reload_loop import run_reload_schedule
from taskq.auth import PgCredential, ReloadSchedule, make_pg_pool_factory
from taskq.testing.clock import FakeClock

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


class _LeaseProvider:
    """Issues a fresh username-bearing pair per call, with the TTL given."""

    def __init__(self, lease_duration: float | None) -> None:
        self.calls = 0
        self.lease_duration = lease_duration

    async def get_pg_credential(self) -> PgCredential:
        self.calls += 1
        return PgCredential(
            password=f"pw-{self.calls}",
            username=f"v-{self.calls}",
            lease_duration=self.lease_duration,
        )


class _FakeSleeper:
    """A ``sleep`` that advances a FakeClock instead of waiting.

    Records the instant each sleep was due to end - the rebuild the loop
    scheduled - and parks after *release_after* sleeps so the test can
    inspect state without the loop spinning.
    """

    def __init__(self, clock: FakeClock, *, release_after: int) -> None:
        self.clock = clock
        self.due_at: list[datetime] = []
        self._remaining = release_after
        self.parked = asyncio.Event()

    async def __call__(self, seconds: float) -> None:
        if self._remaining == 0:
            self.parked.set()
            await asyncio.Event().wait()  # parked until cancelled
        self._remaining -= 1
        due = self.clock.now() + timedelta(seconds=seconds)
        self.due_at.append(due)
        self.clock.move_to(due)


async def _drain(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_rebuild_is_scheduled_at_half_the_granted_lease() -> None:
    """A lease issued at T schedules the rebuild at T + TTL/2, and crossing
    that boundary takes exactly one more credential from the provider."""
    clock = FakeClock(start=_T0)
    provider = _LeaseProvider(lease_duration=3600)
    schedule = ReloadSchedule()
    factory = make_pg_pool_factory("postgresql://h/db", provider, reload_schedule=schedule)
    sleeper = _FakeSleeper(clock, release_after=1)

    with patch("asyncpg.create_pool", new=AsyncMock(side_effect=lambda **_: MagicMock())):
        await factory()  # the first build, at T
        assert provider.calls == 1
        assert schedule.interval == 1800.0

        async def rebuild() -> None:
            await factory()

        task = asyncio.create_task(
            run_reload_schedule(
                schedule, rebuild, trigger=asyncio.Event(), role="test", sleep=sleeper
            )
        )
        await asyncio.wait_for(sleeper.parked.wait(), timeout=5.0)
        await _drain(task)

    assert sleeper.due_at[0] == _T0 + timedelta(seconds=1800)
    assert provider.calls == 2
    assert clock.now() == _T0 + timedelta(seconds=1800)


async def test_a_shorter_lease_on_rebuild_tightens_the_next_wait() -> None:
    """The interval is re-read on every wait: a lease that comes back
    shorter (Vault capping it at the token's remaining TTL) shortens the
    wait from the next tick on."""
    clock = FakeClock(start=_T0)
    provider = _LeaseProvider(lease_duration=3600)
    schedule = ReloadSchedule()
    factory = make_pg_pool_factory("postgresql://h/db", provider, reload_schedule=schedule)
    sleeper = _FakeSleeper(clock, release_after=2)

    with patch("asyncpg.create_pool", new=AsyncMock(side_effect=lambda **_: MagicMock())):
        await factory()

        async def rebuild() -> None:
            provider.lease_duration = 600
            await factory()

        task = asyncio.create_task(
            run_reload_schedule(
                schedule, rebuild, trigger=asyncio.Event(), role="test", sleep=sleeper
            )
        )
        await asyncio.wait_for(sleeper.parked.wait(), timeout=5.0)
        await _drain(task)

    assert [d - _T0 for d in sleeper.due_at] == [
        timedelta(seconds=1800),
        timedelta(seconds=1800 + 300),
    ]


async def test_no_interval_means_trigger_only() -> None:
    """With nothing configured and no lease known there is no timer: the
    loop rebuilds on the trigger alone, and the trigger coalesces - N
    requests during one rebuild yield exactly one follow-up."""
    schedule = ReloadSchedule()
    trigger = asyncio.Event()
    in_rebuild = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    sleeper = MagicMock(side_effect=AssertionError("no interval, so no sleep"))

    async def rebuild() -> None:
        nonlocal calls
        calls += 1
        in_rebuild.set()
        await release.wait()

    task = asyncio.create_task(
        run_reload_schedule(schedule, rebuild, trigger=trigger, role="test", sleep=sleeper)
    )
    trigger.set()
    await asyncio.wait_for(in_rebuild.wait(), timeout=5.0)
    trigger.set()
    trigger.set()
    in_rebuild.clear()
    release.set()
    await asyncio.wait_for(in_rebuild.wait(), timeout=5.0)
    await asyncio.sleep(0)
    assert calls == 2
    await _drain(task)
    sleeper.assert_not_called()


async def test_a_failed_rebuild_is_reported_and_the_loop_continues() -> None:
    """A rebuild that raises leaves the loop running (the old pool is still
    serving) and is reported with its cause; the next tick retries."""
    schedule = ReloadSchedule(configured=10.0)
    clock = FakeClock(start=_T0)
    sleeper = _FakeSleeper(clock, release_after=2)
    outcomes: list[str] = []

    async def rebuild() -> None:
        outcomes.append("called")
        if len(outcomes) == 1:
            raise RuntimeError("token endpoint down")

    with structlog.testing.capture_logs() as logs:
        task = asyncio.create_task(
            run_reload_schedule(
                schedule, rebuild, trigger=asyncio.Event(), role="test", sleep=sleeper
            )
        )
        await asyncio.wait_for(sleeper.parked.wait(), timeout=5.0)
        await _drain(task)

    assert outcomes == ["called", "called"]
    failed = [e for e in logs if e["event"] == "credentials-reload-failed"]
    assert len(failed) == 1
    assert failed[0]["error_type"] == "RuntimeError"
    assert failed[0]["cause"] == "schedule"
    assert failed[0]["role"] == "test"
    reloaded = [e for e in logs if e["event"] == "credentials-reloaded"]
    assert len(reloaded) == 1
    assert reloaded[0]["next_interval"] == 10.0


async def test_a_trigger_is_reported_as_the_cause() -> None:
    schedule = ReloadSchedule()
    trigger = asyncio.Event()
    done = asyncio.Event()

    async def rebuild() -> None:
        done.set()

    with structlog.testing.capture_logs() as logs:
        task = asyncio.create_task(
            run_reload_schedule(schedule, rebuild, trigger=trigger, role="test")
        )
        trigger.set()
        await asyncio.wait_for(done.wait(), timeout=5.0)
        await asyncio.sleep(0)
        await _drain(task)
    reloaded: list[Any] = [e for e in logs if e["event"] == "credentials-reloaded"]
    assert reloaded and reloaded[0]["cause"] == "trigger"
