"""THE TIMER DRILL: the scheduled reconciliation pass runs on the
DURABLE TIMER — a hold whose DB-clock deadline IS the tick.

* fires NO EARLIER than scheduled (the body ENFORCES the schedule —
  even an operator's early nudge is enforced away);
* SURVIVES a worker death mid-hold (the hold row is in Postgres; the
  sweep's expiry arm is the only live timer).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json

import asyncpg
import pytest

from examples.ledgerwatch.client import await_recon_report, submit_reconciliation
from examples.ledgerwatch.models import ReconReport
from examples.ledgerwatch.tests.support import Fleet, wait_for

pytestmark = pytest.mark.timeout(600)

#: The recon timer's delay: 4 seconds (the drill's clock).
FLEET_ENV: dict[str, str] = {"LW_RECON_DELAY_S": "4"}


async def _scheduled_at(pool: asyncpg.Pool, schema: str, flow_id: str) -> dt.datetime:
    """The armed schedule of ONE run, from the step ledger (the
    memoized substep's recorded result) — polled (the arming happens
    when the worker first claims the node). SCOPED BY FLOW: the tests
    run in shuffled order (pytest-randomly) and share the estate — a
    global read would answer with ANOTHER run's schedule."""
    from examples.ledgerwatch.tests.support import wait_for

    async def _armed() -> dt.datetime | None:
        raw = await pool.fetchval(
            f'SELECT result FROM "{schema}".wf_step_ledger'  # noqa: S608  # Why: the schema identifier only.
            " WHERE step_key = 'arm-recon-timer' AND flow_id = $1 AND result IS NOT NULL"
            " ORDER BY id LIMIT 1",
            flow_id,
        )
        if not raw:
            return None
        decoded = json.loads(raw)
        value = decoded["value"] if isinstance(decoded, dict) else decoded
        return dt.datetime.fromisoformat(value)

    return await wait_for(_armed, timeout_s=30, message="the timer never armed")


async def test_recon_fires_no_earlier_than_scheduled(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    claim = await submit_reconciliation(pool, schema, run_key="recon:drill-1")
    assert claim.kind == "created"

    # The ARM (the body's first execution, whenever the worker claims
    # it — a busy estate claims late; the schedule is RELATIVE to the
    # arm, never to the submit).
    scheduled = await _scheduled_at(pool, schema, str(claim.flow_id))

    report = await await_recon_report(pool, schema, str(claim.flow_id), timeout_s=90)
    assert isinstance(report, ReconReport)

    # FIRED NO EARLIER THAN SCHEDULED (the DB clocks' own comparison —
    # the pass row is THIS run's, by the receipt's own pass id; the
    # body's while-loop is the enforcement, this row is the proof).
    rows = await pool.fetch(
        f'SELECT fired_at FROM "{schema}".lw_recon_passes WHERE pass_id = $1',  # noqa: S608  # Why: as above.
        report.pass_id,
    )
    assert rows[0]["fired_at"] >= scheduled, (
        f"the pass fired EARLY: fired {rows[0]['fired_at']} < scheduled {scheduled}"
    )


async def test_recon_enforces_the_schedule_against_an_early_nudge(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    """An operator delivers the timer's hold EARLY — the body waits
    again; the pass still fires at-or-after the schedule."""
    from taskq.workflows import HitlClient

    claim = await submit_reconciliation(pool, schema, run_key="recon:drill-2")
    await asyncio.sleep(0.5)
    scheduled = await _scheduled_at(pool, schema, str(claim.flow_id))

    client = HitlClient(pool, schema=schema)

    async def _hold() -> object | None:
        holds = await client.list(str(claim.flow_id))
        return holds[0] if holds else None

    hold = await wait_for(_hold, timeout_s=30, message="the timer's hold never appeared")
    result = await client.resolve(hold.hold_id, {"note": "an early nudge"})
    assert result.status == "delivered"

    report = await await_recon_report(pool, schema, str(claim.flow_id), timeout_s=90)
    _ = report
    rows = await pool.fetch(
        f'SELECT fired_at FROM "{schema}".lw_recon_passes ORDER BY written_at DESC LIMIT 1'  # noqa: S608  # Why: as above.
    )
    assert rows[0]["fired_at"] >= scheduled, (
        "the nudge moved the pass early — the schedule did not hold"
    )


async def test_recon_timer_survives_worker_death(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    claim = await submit_reconciliation(pool, schema, run_key="recon:drill-3")
    scheduled = await _scheduled_at(pool, schema, str(claim.flow_id))

    # THE HOLD IS LIVE before the crash (the timer's wait is what must
    # survive).
    from taskq.workflows import HitlClient

    client = HitlClient(pool, schema=schema)

    async def _hold() -> object | None:
        holds = await client.list(str(claim.flow_id))
        return holds[0] if holds else None

    await wait_for(_hold, timeout_s=30, message="the timer's hold never appeared")

    # THE CRASH, mid-hold.
    await fleet.kill_all()
    held = await pool.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_signals'  # noqa: S608  # Why: the schema identifier only.
        " WHERE status = 'held' AND workflow_id = $1",
        str(claim.flow_id),
    )
    assert held == 1, "the timer's hold did not survive the death"

    await fleet.start(1)
    report = await await_recon_report(pool, schema, str(claim.flow_id), timeout_s=120)
    assert isinstance(report, ReconReport)
    rows = await pool.fetch(
        f'SELECT fired_at FROM "{schema}".lw_recon_passes ORDER BY written_at DESC LIMIT 1'  # noqa: S608  # Why: as above.
    )
    assert rows[0]["fired_at"] >= scheduled, "the resurrected timer fired early"
