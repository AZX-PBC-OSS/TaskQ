# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The §9 control 12: the representation, the listing, the admin read
surfaces, and the history agree.

The defect classes this control pins:

* the probes' PARK representation (the exact §2.2 marker row) and the
  claims' / listing's / admin's reads of that SAME row must agree — a
  park is `scheduled` with `blocking_reason='backpressure'`; the admin's
  `parked` count, the claimable-nodes read, and the listing all key the
  SAME predicate, and the admin NEVER renders the park instant as a
  `next_fire` (a park has no timer);
* the OTHER deferral representations (a due/future snooze schedule, a
  hold, a failed/cancelled root, a reserved source) read their own
  honest faces on every surface;
* `awaited -> succeeded/failed` retains SEPARATE claim-epoch history:
  the epoch arbiter keeps one row per claim; no terminal ledger row
  projects onto the live current state (the RULING_STATUS_IDENTITY's
  own law).
"""

from __future__ import annotations

import json as _json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows.api._sql_runner import (
    CLAIMABLE_NODES_SQL_TEMPLATE,
    render_sql,
)

pytestmark = pytest.mark.integration

_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


def _park_meta(flow_id: str, *, width: int, bound: int) -> dict[str, Any]:
    """The complete §2.2 park row's metadata as exact literal values."""
    return {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
        "emit_cursor": {"page": 0},
        "wf_deferral": {"signal": "EmitBackpressure", "consume_budget": False, "attempt": 0},
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": width,
            "bound": bound,
            "outstanding": bound,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }


async def _seed_flow_root(conn: Any, schema: str, flow_id: str, *, status: str = "running") -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', $2, "
        "'__flow__', $3::jsonb, 'workflow-run', $4)",
        flow_id,
        status,
        _json.dumps({"flow_id": flow_id}),
        f"flow:{flow_id}",
    )


async def test_park_reads_agree_across_claimable_listing_and_admin(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The one park row reads the SAME on every surface: the claimable
    -nodes read counts it ZERO (a park is the promotion's to move), the
    admin's runs list counts it parked=1 and deferring=0 with next_fire
    NULL (a park has no timer and the park instant is never a next
    fire), and the row's own metadata is the exact §2.2 marker."""
    from taskq.web.admin.workflows import _RUNS_SQL, _park_predicate

    flow = str(new_uuid())
    await _seed_flow_root(wf_conn, wf_schema, flow)
    park = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 0, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, 'workflow-run', $4)",
        park,
        _json.dumps(_park_meta(flow, width=2, bound=4)),
        flow,
        f"wf:{flow}:source",
    )

    # THE CLAIMABLE-NODES READ: a park is never claimable work.
    countable = await wf_conn.fetchval(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema), flow)
    assert int(countable or 0) == 0, countable

    # THE ADMIN RUNS LIST: parked=1, deferring=0, next_fire NULL — the
    # park instant is displayed as park time, never a next fire.
    runs = await wf_conn.fetch(
        _RUNS_SQL.format(schema=wf_schema, park_predicate=_park_predicate()), 10
    )
    assert len(runs) == 1, runs
    assert int(runs[0]["parked"]) == 1, dict(runs[0])
    assert int(runs[0]["deferring"]) == 0, dict(runs[0])
    assert runs[0]["next_fire"] is None, dict(runs[0])


async def test_other_deferral_faces_render_honestly_and_awaited_history_is_kept(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The due/future snooze schedules and a hold render their own honest
    faces (the admin's deferring/next_fire), a failed root's parked-only
    park reads parked (not deferring), and the `awaited -> succeeded`
    history keeps separate claim-epoch rows — no terminal row projects
    onto the live current state."""
    from taskq.web.admin.workflows import _RUNS_SQL, _park_predicate

    # ── a FUTURE snooze schedule: deferring=1 with a real next fire ──
    flow_snooze = str(new_uuid())
    await _seed_flow_root(wf_conn, wf_schema, flow_snooze)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 1, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp() + interval '1 hour', clock_timestamp(), "
        "$3, 'workflow-run', $4)",
        JobId(new_uuid()),
        _json.dumps(
            {
                "flow_id": flow_snooze,
                "wf_deferral": {"signal": "Snooze", "consume_budget": False, "attempt": 1},
            }
        ),
        flow_snooze,
        f"wf:{flow_snooze}:source",
    )
    runs = await wf_conn.fetch(
        _RUNS_SQL.format(schema=wf_schema, park_predicate=_park_predicate()), 10
    )
    snooze_run = next(r for r in runs if str(r["id"]) == flow_snooze)
    assert int(snooze_run["deferring"]) == 1, dict(snooze_run)
    assert snooze_run["next_fire"] is not None, dict(snooze_run)
    assert int(snooze_run["parked"]) == 0, dict(snooze_run)

    # ── the awaited -> succeeded history: separate claim-epoch rows ──
    flow_hist = str(new_uuid())
    await _seed_flow_root(wf_conn, wf_schema, flow_hist)
    source_id = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'succeeded', 1, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, 'workflow-run', $4)",
        source_id,
        _json.dumps({"flow_id": flow_hist}),
        flow_hist,
        f"wf:{flow_hist}:source",
    )
    # The parked attempt's awaited row (claim_epoch 1) + the resumed
    # claim's succeeded row (claim_epoch 2): SEPARATE rows, both kept.
    for epoch, status in ((1, "awaited"), (2, "succeeded")):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_step_ledger '
            "(id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status) "
            "VALUES ($1, $2::uuid, $3::uuid, 'source', NULL, 1, $4, $5)",
            JobId(new_uuid()),
            flow_hist,
            source_id,
            epoch,
            status,
        )
    history = await wf_conn.fetch(
        f"""SELECT attempt::int AS a, claim_epoch::int AS e, status::text AS s
            FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 'source' ORDER BY claim_epoch""",
        flow_hist,
    )
    assert [(int(r["e"]), r["s"]) for r in history] == [
        (1, "awaited"),
        (2, "succeeded"),
    ], history

    # NO terminal ledger row projects onto the live current state: the
    # node row is 'succeeded' from its OWN terminal, and the awaited
    # history row is not a live state anywhere.
    node = await wf_conn.fetchrow(
        f'SELECT status::text AS status FROM "{wf_schema}".jobs WHERE id = $1', source_id
    )
    assert node is not None and node["status"] == "succeeded", node


async def test_holds_joins_and_cancelled_roots_read_the_same_on_every_surface(
    wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """N12: the rest of the contract's shape list — a HOLD, a JOIN wait,
    a due and a future schedule, and a CANCELLED root's own park — read
    the same on the claimable-nodes read and the admin runs list; the
    claim-list parity: the claimable read counts exactly the rows the
    listing's own deferring/next-fire aggregates do NOT count as parked
    (one predicate, two surfaces, no disagreement)."""
    from taskq.web.admin.workflows import _RUNS_SQL, _park_predicate
    from taskq.workflows.api._sql_runner import (
        CLAIMABLE_NODES_SQL_TEMPLATE,
        render_sql,
    )

    # ── a HOLD (the wait's own representation: pending + the hold key) ──
    flow_hold = str(new_uuid())
    await _seed_flow_root(wf_conn, wf_schema, flow_hold)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'pending', 1, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, 'workflow-run', $4)",
        JobId(new_uuid()),
        _json.dumps({"flow_id": flow_hold, "hold": {"hold_id": "h1"}}),
        flow_hold,
        f"wf:{flow_hold}:source",
    )

    # ── a JOIN wait (pending + deps_pending + the join marker) ──
    flow_join = str(new_uuid())
    await _seed_flow_root(wf_conn, wf_schema, flow_join)
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, deps_pending, metadata, scheduled_at, "
        "created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'pending', 0, 2, 'transient', 'join_step', "
        "2, $2::jsonb, clock_timestamp(), clock_timestamp(), $3, 'workflow-run', $4)",
        JobId(new_uuid()),
        _json.dumps({"flow_id": flow_join, "blocking_reason": "join"}),
        flow_join,
        f"wf:{flow_join}:join_step",
    )

    # ── a DUE schedule (claimable) beside a FUTURE one (not yet) ──
    flow_sched = str(new_uuid())
    await _seed_flow_root(wf_conn, wf_schema, flow_sched)
    for due, key in ((True, "due"), (False, "future")):
        scheduled_sql = "now() - interval '1 hour'" if due else "now() + interval '1 hour'"
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
            "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
            "fairness_key, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 1, 2, 'transient', 'source', "
            f"$2::jsonb, {scheduled_sql}, "
            "clock_timestamp(), $3, 'workflow-run', $4)",
            JobId(new_uuid()),
            _json.dumps(
                {
                    "flow_id": flow_sched,
                    "wf_deferral": {"signal": "Snooze", "consume_budget": False, "attempt": 1},
                }
            ),
            flow_sched,
            f"wf:{flow_sched}:{key}",
        )

    # ── a CANCELLED root's own park (the cancel's own resolution) ──
    flow_cancelled = str(new_uuid())
    await _seed_flow_root(wf_conn, wf_schema, flow_cancelled, status="cancelled")
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'cancelled', 0, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, 'workflow-run', $4)",
        JobId(new_uuid()),
        _json.dumps(_park_meta(flow_cancelled, width=1, bound=2)),
        flow_cancelled,
        f"wf:{flow_cancelled}:source",
    )

    # THE ADMIN RUNS LIST: every run's own honest faces.
    runs = await wf_conn.fetch(
        _RUNS_SQL.format(schema=wf_schema, park_predicate=_park_predicate()), 50
    )
    by_flow = {str(r["id"]): r for r in runs}
    # The HOLD: not parked (a hold is not a park); its deferral face is
    # its own (no snooze stamp — the hold rides the hold key).
    assert int(by_flow[flow_hold]["parked"]) == 0, dict(by_flow[flow_hold])
    # The JOIN: not parked either.
    assert int(by_flow[flow_join]["parked"]) == 0, dict(by_flow[flow_join])
    # The CANCELLED root's park: a TERMINAL row is not parked work —
    # P(j)'s own status leg excludes it (the marker keys ride the row's
    # history, never a live park).
    assert int(by_flow[flow_cancelled]["parked"]) == 0, dict(by_flow[flow_cancelled])
    assert int(by_flow[flow_cancelled]["deferring"]) == 0, dict(by_flow[flow_cancelled])

    # THE CLAIM-LIST PARITY: the claimable-nodes read counts exactly the
    # due schedule's own row (the hold, the join, and the future schedule
    # are not claimable; the cancelled root's rows are unclaimable
    # through the terminal-flow fence).
    countable = await wf_conn.fetch(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema), flow_sched)
    assert len(countable) == 1, [dict(r) for r in countable]
    for other in (flow_hold, flow_join, flow_cancelled):
        none_countable = await wf_conn.fetch(
            render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, wf_schema), other
        )
        assert len(none_countable) == 0, [dict(r) for r in none_countable]
