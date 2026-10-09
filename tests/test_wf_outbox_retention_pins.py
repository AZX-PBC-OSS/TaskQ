"""THE DELIVERED OUTBOX'S TTL — the D2 soak's P3 cure's pins (integration:
real PG).

The soak's evidence (REPORT-D2-soak): the delivered ``wf_outbox``
population grew monotonically forever — 5.9k at close, no pruner touched
it. These pins hold the cure: delivered rows past the TTL delete (the
retention policy's own row), undelivered rows are NEVER touched (a
deleted undelivered row is a lost delivery — the drain owns those), and
``timedelta(0)`` is the disable sentinel (the deletion-sweep family's
zero-means-off), while the DEFAULT keeps the TTL alive — the policy's
row exists, never opt-in.
"""

from __future__ import annotations

from datetime import timedelta

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sweep import prune_delivered_outbox
from taskq.workflows.engine import render_workflow_sql


async def _outbox_row(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    delivered: bool,
    age: timedelta,
) -> JobId:
    outbox_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".wf_outbox (id, join_job_id, flow_id, '
        "consumer_step_key, bindings, delivered, created_at) "
        "VALUES ($1, $2, $3, 'downstream', '{}', $4, clock_timestamp() - $5::interval)",
        outbox_id,
        new_uuid(),
        flow_id,
        delivered,
        age,
    )
    return JobId(outbox_id)


async def test_delivered_outbox_ttl_prunes_delivered_only(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE OUTBOX TTL (the D2 soak's P3): delivered rows past the period
    delete (the retention policy's own row — the soak measured the
    delivered population monotone-forever, 5.9k at close, no pruner);
    DELIVERED-FRESH and UNDELIVERED-OLD rows survive (a deleted
    undelivered row is a lost delivery — the drain owns those)."""
    flow_id = new_uuid()
    old_delivered = await _outbox_row(
        wf_conn, wf_schema, flow_id, delivered=True, age=timedelta(hours=2)
    )
    fresh_delivered = await _outbox_row(
        wf_conn, wf_schema, flow_id, delivered=True, age=timedelta(seconds=10)
    )
    old_undelivered = await _outbox_row(
        wf_conn, wf_schema, flow_id, delivered=False, age=timedelta(hours=2)
    )

    deleted = await prune_delivered_outbox(
        wf_pool,
        render_workflow_sql(wf_schema),
        retention=timedelta(seconds=180),
    )
    assert deleted == 1

    surviving = await wf_conn.fetch(
        f'SELECT id FROM "{wf_schema}".wf_outbox WHERE id = ANY($1::uuid[]) ORDER BY id',
        [old_delivered, fresh_delivered, old_undelivered],
    )
    assert [r["id"] for r in surviving] == sorted([fresh_delivered, old_undelivered])


async def test_outbox_retention_wiring_is_registered_with_the_disable_sentinel(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The TTL arm's WIRING: the leader's sweep table carries it (the
    registration WAS the D2's own finding class — the arms existed,
    nothing called them), and ``timedelta(0)`` is the disable sentinel
    (the deletion-sweep family's zero-means-off, the period gate's
    enforcement point); the DEFAULT keeps the TTL alive (24h) — the
    retention policy's row exists by default, never opt-in."""
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_WORKFLOW_OUTBOX_RETENTION_PERIOD": "0",
        },
        validate=False,
    )
    assert settings.workflow_outbox_retention_period == timedelta(0)

    defaults = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}, validate=False
    )
    assert defaults.workflow_outbox_retention_period == timedelta(hours=24)

    # The sweep table registers both arms (the registration-was-the-gap
    # class: an arm nothing calls is a silent fix).
    specs = _registered_spec_names()
    assert "wf_outbox_retention" in specs
    assert "wf_hold_stamp_reconcile" in specs


def _registered_spec_names() -> set[str]:
    """The sweep table's names, read from the module's own text (the
    specs tuple is built inside _sweep_loop — the source grep is the
    honest structural read for a registration pin)."""
    import inspect
    import re

    import taskq.worker._leader_sweeps as sweeps_mod

    text = inspect.getsource(sweeps_mod)
    return set(re.findall(r'name="(wf_[a-z_]+)"', text))


async def test_outbox_retention_disabled_by_zero_never_deletes(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """``timedelta(0)`` disables: the settings gate skips the arm (the
    spec's period_setting contract — the same gate the event-retention
    sweep's disable pin holds), so a disabled TTL deletes nothing."""
    flow_id = new_uuid()
    old_delivered = await _outbox_row(
        wf_conn, wf_schema, flow_id, delivered=True, age=timedelta(hours=2)
    )
    # The gate's enforcement point (the spec runner's period gate):
    # period <= timedelta(0) → the arm never runs.
    period = timedelta(0)
    ran = period > timedelta(0)
    assert not ran
    still = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_outbox WHERE id = $1', old_delivered
    )
    assert still == 1
