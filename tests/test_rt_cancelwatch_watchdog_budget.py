"""Red-team: watchdog budget arithmetic in the heartbeat/cancel interplay.

Two attacks:

1. **``watchdog_loop_lag_warn_budget`` is not validated against
   ``watchdog_loop_lag_budget``** (settings ``post_load`` checks only
   ``budget + heartbeat_interval < lock_lease`` and ``budget >
   check_interval``).  A warn budget at or above the terminal budget
   silently disables tier 1 - the worker is force-exited with no prior
   lag warning - exactly the failure mode the codebase already rejects
   for ``ShutdownWatchdog``'s ``dump_after_fraction`` ("at 1.0 the
   deadline trip always fires first, silently disabling straggler dumps").
   A budget pair the validator cannot distinguish from a healthy one is
   a misconfiguration the operator will only meet at the os._exit.

2. **A cancel-drain tick longer than the heartbeat loop's staleness
   budget reads as a dead loop.**  ``heartbeat_loop`` registers
   ``liveness.tick("heartbeat", period=interval)`` once per iteration,
   and the cancel controller can then run an UNBOUNDED number of
   escalation UPDATE + event INSERT round trips inside that one tick
   (every active job past its cancel grace escalates in the same
   transaction - a bulk cancel makes them all due together).  Detector
   2's budget for the loop is ``max(interval * grace_factor,
   stale_floor)``; a healthy drain that merely takes longer than that
   (each round trip costs real seconds on a loaded PG) force-exits the
   worker mid-drain.  The leader sweep's drain already carries the fix
   pattern (``_drain_bounded`` ticks liveness between calls "so detector
   2 cannot age the loop out during a long drain"); the cancel drain has
   no such ticking.
"""

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend
from taskq.worker._watchdog import LoopLiveness
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool

_FAKE_DSN = "postgresql://fake:fake@fake:5432/fake"


def _ws(**overrides: str | float) -> WorkerSettings:
    data: dict[str, str] = {
        "TASKQ_PG_DSN": _FAKE_DSN,
        "TASKQ_LOCK_LEASE": "360",
        "TASKQ_TERMINATION_GRACE_PERIOD": "360",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    for k, v in overrides.items():
        data[f"TASKQ_{k}"] = str(v)
    return WorkerSettings.load_from_dict(data)


# ── 1. warn budget vs terminal budget ───────────────────────────────────


@pytest.mark.parametrize(
    ("warn_budget", "lag_budget"),
    [
        ("40.0", "30.0"),  # warn strictly above terminal: tier 1 can never fire
        ("30.0", "30.0"),  # equality: tier-1 window is zero-width
    ],
)
def test_warn_budget_at_or_above_lag_budget_is_rejected(warn_budget: str, lag_budget: str) -> None:
    """A tier-1 budget that cannot fire before tier 2 must fail at load.

    Contract: ``watchdog_loop_lag_warn_budget`` must load only when it is
    strictly below ``watchdog_loop_lag_budget`` - otherwise the terminal
    lag trip force-exits with zero prior warning, the same silent-disable
    failure ``dump_after_fraction``'s (0, 1) validation exists to prevent.
    Current behavior: the pair loads cleanly, so the misconfiguration is
    only discovered at the os._exit.
    """
    from dotenvmodel import ValidationError

    with pytest.raises(ValidationError, match="warn_budget"):
        _ws(
            WATCHDOG_LOOP_LAG_WARN_BUDGET=warn_budget,
            WATCHDOG_LOOP_LAG_BUDGET=lag_budget,
        )


def test_warn_budget_below_lag_budget_loads() -> None:
    """Boundary: the healthy pair (warn 5 < budget 30) loads untouched."""
    settings = _ws(
        WATCHDOG_LOOP_LAG_WARN_BUDGET="5.0",
        WATCHDOG_LOOP_LAG_BUDGET="30.0",
    )
    assert settings.watchdog_loop_lag_warn_budget == 5.0
    assert settings.watchdog_loop_lag_budget == 30.0


# ── 2. cancel drain vs detector-2 staleness budget ──────────────────────


class _FakeWallClock:
    """Float clock advanced by the fake connection per round trip."""

    def __init__(self) -> None:
        self.t = 1_000.0

    def __call__(self) -> float:
        return self.t


class _SlowRecorderConn:
    """Mock asyncpg.Connection charging 3 wall-clock seconds per round trip
    (a loaded PG's realistic per-statement latency), escalations applying."""

    def __init__(self, clock: _FakeWallClock, poll_rows: list[dict[str, object]]) -> None:
        self._clock = clock
        self._poll_rows = poll_rows

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self._clock.t += 3.0
        return list(self._poll_rows)

    async def execute(self, sql: str, *args: object) -> str:
        self._clock.t += 3.0
        return "UPDATE 1"


class _StubPayload(BaseModel):
    """Minimal payload for a cancel-path JobContext."""


def _make_ctx() -> JobContext[BaseModel]:
    from datetime import UTC, datetime

    import structlog

    from taskq.obs import bind_job_context
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    backend = InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))
    return JobContext(
        job_id=new_uuid(),
        actor="test",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=_StubPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=new_uuid(),
            actor="test",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


async def test_long_cancel_drain_does_not_read_as_stale_heartbeat_loop() -> None:
    """A tick draining 12 due escalations must not trip detector 2.

    Interleaving: ``heartbeat_loop`` ticks liveness at the top of its
    iteration, then the cancel hook drains every past-grace escalation in
    one transaction.  With the default budget arithmetic
    (``max(interval * grace_factor, stale_floor)`` = 50s at
    interval=10), 12 jobs x (escalation UPDATE + audit INSERT) x 3s of
    per-round-trip latency = 72s of HEALTHY work - and detector 2
    force-exits the worker for a stale "heartbeat" registration.
    """
    clock = _FakeWallClock()
    liveness = LoopLiveness(grace_factor=5.0, stale_floor=10.0, clock=clock)  # type: ignore[arg-type]
    ws = _ws(
        HEARTBEAT_INTERVAL="10",
        LOCK_LEASE="7200",
        TERMINATION_GRACE_PERIOD="7200",
        CANCELLATION_GRACE_PERIOD="0.0",
        # Cleanup grace past any elapsed here so the drain is pure
        # escalation work (no abandons queued in-tx).
        CLEANUP_GRACE_PERIOD="3600.0",
    )
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
        liveness=liveness,
    )
    worker_id = new_uuid()

    # 12 jobs all past the (zero) cancel grace - a bulk cancel's worth of
    # due escalations in one tick.
    poll_rows: list[dict[str, object]] = []
    tasks: list[asyncio.Task[object]] = []
    for _ in range(12):
        job_id = new_job_id()
        task = asyncio.get_running_loop().create_task(asyncio.sleep(3600))
        tasks.append(task)
        await deps.active_jobs.register(job_id, task, _make_ctx())
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 1.0
        poll_rows.append({"id": job_id, "cancel_phase": 1})

    # The heartbeat loop's own registration, as heartbeat_loop issues it.
    liveness.tick("heartbeat", period=10.0)

    controller = make_cancel_controller(deps, worker_id, FakeBackend())  # type: ignore[arg-type]
    conn: Any = _SlowRecorderConn(clock, poll_rows)
    await controller.run_in_tx(conn)

    escalated = sum(
        1 for active in deps.active_jobs.all() if active.cancel_phase >= CancelPhase.FORCED
    )
    assert escalated == 12, "fixture broken: every due escalation must have fired"

    assert liveness.stale() == [], (
        "Contract: a heartbeat tick that is healthily draining its cancel queue "
        "must not read as a stale loop - detector 2's terminal trip would "
        "force-exit the worker mid-drain (and the mid-tick lease renewals that "
        "already happened mean the sweep cannot yet reclaim, so the exit throws "
        "away a drain that was succeeding). The leader sweep's own drain carries "
        "the pattern this requires (_drain_bounded ticks liveness between calls); "
        f"the cancel drain does not: after {escalated} escalations the "
        f"'heartbeat' registration is {clock.t - 1000.0:.0f}s old against a "
        "50s budget."
    )

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
