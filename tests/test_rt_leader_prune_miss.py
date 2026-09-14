"""Red-team pin: the prune loop's fire-second leadership gate must not cost a day.

``_prune_loop`` (``src/taskq/worker/_leader_sweeps.py``) wakes at the once-daily
cron fire and gates on ``is_leader`` BEFORE attempting: a wake that finds the pod
leaderless for an instant ``continue``s back to the top of the loop, where the
next fire is recomputed from the cron expression — TOMORROW.  A seconds-scale
leadership flap that happens to span the fire second therefore becomes a 24-hour
prune miss with no log, no metric, no retry: the failure half of the loop's own
policy (``_PRUNE_RETRY_BACKOFF_INITIAL_SECS`` — "a prune that keeps failing under
load does not wait for tomorrow") is armed only for FAILED attempts, not for
missed ones.

Contract under test (RED until fixed): a prune wake that finds itself leaderless
at the fire second must retry within a bounded window (the shared backoff ladder
is the obvious arm), or at minimum record the miss loudly — a 1-second leadership
gap must not silently defer retention work by 24 hours.

Driven at the loop seam with fakes (no PG): the loop's own scheduling decision is
the unit under test, so the croniter call is scripted (fire under test 3 s out;
from a base at the just-missed fire second a daily cron's next slot is exactly
fire+24h — what the script returns), the advisory-lock acquire is a counted fake
that reports contention (the cheapest observable "attempt"), and the module-level
backoff initial is shrunk to 1 s — the documented shrink seam
("Module-level (not settings) so tests shrink it without threading a knob through
every loop").
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

import pytest
import structlog

import taskq.worker._leader_sweeps as sweeps
from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.testing.settings import make_integration_settings
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _prune_loop

#: The bounded window the contract demands for a missed-fire retry. With the
#: backoff initial shrunk to 1 s, a fixed loop must attempt (or log) within the
#: first backoff rung or two — 10 s is generous to that fix and merciless to a
#: 24-hour defer.
_MISSED_FIRE_RETRY_WINDOW_SECS: float = 10.0

#: Seconds from loop start to the scheduled fire under test.
_FIRE_DELAY_SECS: float = 3.0


class _ScriptedCroniter:
    """Stand-in for one ``croniter(expr, base).get_next(datetime)`` call.

    The first call returns ``base + _FIRE_DELAY_SECS`` (the scheduled fire under
    test); every later call returns ``fire1 + 24 h`` — exactly what a daily cron
    expression's ``get_next`` yields from a base at the just-missed fire second
    (the next slot is tomorrow's), which is the defer the pin attacks.
    """

    def __init__(self, mod: _CroniterShim, base: datetime) -> None:
        self._mod = mod
        self._base = base

    def get_next(self, ret_type: type[datetime]) -> datetime:
        assert ret_type is datetime
        if self._mod.fire1 is None:
            self._mod.fire1 = self._base + timedelta(seconds=_FIRE_DELAY_SECS)
            fire = self._mod.fire1
        else:
            fire = self._mod.fire1 + timedelta(days=1)
        self._mod.calls.append((self._base, fire))
        return fire


class _CroniterShim:
    """Module-shaped stand-in for ``croniter as cr`` in _leader_sweeps."""

    def __init__(self) -> None:
        self.fire1: datetime | None = None
        self.calls: list[tuple[datetime, datetime]] = []

    def croniter(self, expr: str, base: datetime) -> _ScriptedCroniter:
        return _ScriptedCroniter(self, base)


class _FakeLiveness:
    """Detector-2 registry stub: the prune loop only ticks/forgets it."""

    def tick(self, name: str, *, period: float) -> None:
        return None

    def forget(self, name: str) -> None:
        return None


class _FakePruneConn:
    """Pool conn stub: counts advisory-lock attempts, reports contention."""

    def __init__(self) -> None:
        self.lock_attempts = 0

    async def fetchval(self, sql: str, *args: object) -> bool:
        self.lock_attempts += 1
        return False

    async def execute(self, *args: object, **kwargs: object) -> str:
        return "OK"


class _FakeAcquire:
    def __init__(self, conn: _FakePruneConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakePruneConn:
        return self._conn

    async def __aexit__(self, *args: object) -> bool:
        return False


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakePruneConn()

    def acquire(self, timeout: float | None = None) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


class _FakeDeps:
    """The WorkerDeps subset _prune_loop touches, on fakes."""

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self.is_leader = asyncio.Event()
        self.liveness = _FakeLiveness()
        self.dispatcher_pool = _FakePool()


class _FakeBackend:
    async def prune_old_batches(self, cutoff: object) -> int:
        return 0


async def test_prune_miss_at_fire_second_must_not_silently_defer_24h(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = make_integration_settings(
        "postgresql://user:pass@localhost:5432/tlrt_no_pg",  # unused: the pool is a fake
        schema_name="tlrt_prune_miss",
        prune_cron_expr="0 3 * * *",
    )
    fake_deps = _FakeDeps(settings)
    ctx = SweepContext(
        deps=fake_deps,  # type: ignore[arg-type]  # Why: duck-typed deps — only settings/is_leader/liveness/dispatcher_pool are touched by _prune_loop.
        backend=_FakeBackend(),  # type: ignore[arg-type]  # Why: duck-typed backend — prune_old_batches is never reached (the lock fake reports contention).
        clock=SystemClock(),
        worker_id=new_uuid(),
    )
    shim = _CroniterShim()
    monkeypatch.setattr(sweeps, "cr", shim)
    # The documented shrink seam: a fixed loop retrying a missed fire on this
    # ladder wakes within ~1 s, not 60 s.
    monkeypatch.setattr(sweeps, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 1.0)

    shutdown = asyncio.Event()
    fake_deps.is_leader.set()
    with structlog.testing.capture_logs() as captured:
        t0 = time.monotonic()
        task = asyncio.create_task(_prune_loop(ctx, shutdown), name="rt-prune-miss")
        try:
            # A ~1.3 s leadership flap spanning the fire second (fire at t0+3):
            # leaderless from t0+2, leader again from t0+3.3.
            await asyncio.sleep(2.0)
            fake_deps.is_leader.clear()
            await asyncio.sleep(1.3)
            fake_deps.is_leader.set()
            t_miss = t0 + _FIRE_DELAY_SECS
            miss_index = len(captured)

            # The contract: within the bounded window of the missed fire, the
            # loop must either attempt the prune again (the counted advisory-lock
            # acquire) or record the miss loudly (any prune-kind log event) —
            # not silently sleep to tomorrow's slot.
            outcome: str | None = None
            while time.monotonic() - t_miss < _MISSED_FIRE_RETRY_WINDOW_SECS:
                if fake_deps.dispatcher_pool.conn.lock_attempts > 0:
                    outcome = "re-attempted the prune within the window"
                    break
                if any(e.get("kind") == "prune" for e in captured[miss_index:]):
                    outcome = "recorded the missed fire loudly"
                    break
                await asyncio.sleep(0.05)

            defer_hours: float | None = None
            if len(shim.calls) >= 2:
                _base2, next_fire = shim.calls[-1]
                assert shim.fire1 is not None
                defer_hours = (next_fire - shim.fire1).total_seconds() / 3600.0
            assert outcome is not None, (
                "CONTRACT: a prune wake that finds the pod leaderless at the fire "
                "second must retry within a bounded window (or at minimum record the "
                "miss loudly) — a seconds-scale leadership flap must not cost a day "
                "of retention drift. TODAY: the is_leader gate at _leader_sweeps.py "
                "(right after the wake) `continue`s with no log, and the loop top "
                "recomputes the next fire from the daily cron as TOMORROW"
                + (f" ({defer_hours:.1f}h away in this run)" if defer_hours is not None else "")
                + f" — a ~1.3 s leadership gap became a 24 h prune miss, unlogged "
                f"(captured prune events since the miss: "
                f"{[e.get('event') for e in captured[miss_index:]]}); the loop's own "
                "failure policy retries failed attempts intra-day "
                "(_PRUNE_RETRY_BACKOFF_INITIAL_SECS) but a MISSED fire waits for "
                "tomorrow's slot"
            )
        finally:
            shutdown.set()
            await asyncio.wait_for(task, timeout=5.0)
