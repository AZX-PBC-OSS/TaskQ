# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The D7/D8/D10 controls: the marker sweep's rotation and per-flow
isolation, the admin read surfaces' park law, and the capacity record's
retention.

* **D7.** The exact-marker sweep is an ordered KEYSET rotation over the
  ``jobs_wf_emit_park_idx`` partial index with each flow's repair in
  its OWN transaction: more than 100 marked flows all repair within
  ``ceil(N/100) + 1`` selections (no flow starves), and one malformed
  marker poisons only its own flow (bounded diagnostic, retried on a
  later selection) while every other flow's repair commits.
* **D8.** The admin runs list keys the park exclusion/count on the
  shared P(j) predicate: a re-pended reserved source (a live deferral
  with the blob but no park marker) renders honestly (deferring + next
  fire from its own due time), never hidden, never parked.
* **D10.** The capacity record's retention lifetime is the run's: the
  rootless-family walk removes the record exactly when the run's own
  rows were retained away, and never while any row of the run still
  exists.
"""

from __future__ import annotations

import json as _json
from typing import Any

import pytest

from taskq._ids import new_uuid
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration

_DEFERRED_AT = "2026-11-15T09:30:00.123456Z"


def _park_meta(flow_id: str, *, width: int, bound: int, cursor: bool = True) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "flow_id": flow_id,
        "blocking_reason": "backpressure",
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
    if cursor:
        meta["emit_cursor"] = {"page": 0}
    return meta


async def _seed_marked_flow(
    conn: Any,
    schema: str,
    *,
    width: int = 1,
    bound: int = 10,
    poison: bool = False,
    with_intent: bool = False,
) -> str:
    """One flow root + one exact park (+ optionally its intent record)."""
    flow_id = str(new_uuid())
    meta = _park_meta(flow_id, width=width, bound=bound)
    if poison:
        meta["wf_emit_backpressure"]["page_width"] = "oops"
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', "
        "'__flow__', $2::jsonb, 'workflow-run', $3)",
        flow_id,
        _json.dumps({"flow_id": flow_id}),
        f"flow:{flow_id}",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 0, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3, 'workflow-run', $4)",
        str(new_uuid()),
        _json.dumps(meta),
        flow_id,
        f"wf:{flow_id}:source",
    )
    if with_intent:
        await conn.execute(
            f'INSERT INTO "{schema}".wf_emit_capacity '
            "(flow_id, bound, wake_generation, handled_generation) VALUES ($1::uuid, $2, 0, 0)",
            flow_id,
            bound,
        )
    return flow_id


def _wsql(schema: str) -> WorkflowSql:
    return WorkflowSql.build(schema)


async def test_marker_sweep_repairs_all_marked_flows_within_the_rotation_bound(
    wf_conn: Any, wf_schema: str, wf_pool: Any
) -> None:
    """D7: 150 marked flows without intents ALL repair within
    ceil(150/100) + 1 = 3 sweep selections — the pre-fix defect repaired
    the same first 100 forever and starved 50."""
    from taskq.workflows._emit import _SWEEP_CURSORS, sweep_emit_markers

    wsql = _wsql(wf_schema)
    for _ in range(150):
        await _seed_marked_flow(wf_conn, wf_schema)
    _SWEEP_CURSORS.pop(wf_schema, None)

    repaired_total = 0
    for _ in range(3):
        repaired_total += await sweep_emit_markers(wf_pool, wsql)
    capacity = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity')
    assert int(capacity) == 150, capacity
    assert repaired_total == 150, repaired_total
    # The rotation actually advanced: the second call repaired the LATER
    # flows, not a re-repair of the first 100 (the per-call receipts sum
    # to the whole population within the bound).
    _SWEEP_CURSORS.pop(wf_schema, None)


async def test_marker_sweep_quarantines_a_poison_marker_and_repairs_the_rest(
    wf_conn: Any, wf_schema: str, wf_pool: Any
) -> None:
    """D7: one malformed marker poisons only its own flow — the other 99
    repair in the same sweep pass (the pre-fix defect: every call raised
    and left 0 capacity rows)."""
    from taskq.workflows._emit import _SWEEP_CURSORS, sweep_emit_markers

    wsql = _wsql(wf_schema)
    for i in range(100):
        await _seed_marked_flow(wf_conn, wf_schema, poison=(i == 0))
    _SWEEP_CURSORS.pop(wf_schema, None)

    repaired = await sweep_emit_markers(wf_pool, wsql)
    assert repaired == 99, repaired
    capacity = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity')
    assert int(capacity) == 99, capacity
    # The poison is quarantined, not dropped: a later selection retries
    # it (still failing, still bounded), and the healthy 99 stay intact.
    repaired_again = await sweep_emit_markers(wf_pool, wsql)
    assert repaired_again == 0, repaired_again
    capacity = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity')
    assert int(capacity) == 99, capacity
    _SWEEP_CURSORS.pop(wf_schema, None)


async def test_capacity_record_retires_with_its_run_and_never_before(
    wf_conn: Any, wf_schema: str, wf_pool: Any
) -> None:
    """D10: the capacity record's retention lifetime is the run's — the
    rootless-family walk removes it exactly when the run's rows were
    retained away, and never while any row of the run exists."""
    from taskq.workflows._sweep import prune_wf_capacity_with_run

    wsql = _wsql(wf_schema)
    flow = await _seed_marked_flow(wf_conn, wf_schema, with_intent=True)

    # The run's rows all exist: the walk removes nothing.
    deleted = await prune_wf_capacity_with_run(wf_pool, wsql, batch_size=100)
    assert deleted == 0, deleted
    still = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow,
    )
    assert int(still) == 1, still

    # THE RUN RETAINED AWAY: the run's root and node rows are gone. The
    # rootless walk's own cursor law: a row VISITED before it became
    # doomed is re-visited after the exhausted pass's wraparound (the
    # walk moves past live rows; the rewind restarts it) — the family's
    # documented advance law, bounded by the batch.
    await wf_conn.execute(
        f"DELETE FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid",
        flow,
    )
    deleted_total = 0
    for _ in range(3):
        deleted_total += await prune_wf_capacity_with_run(wf_pool, wsql, batch_size=100)
        if deleted_total >= 1:
            break
    assert deleted_total == 1, deleted_total
    gone = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow,
    )
    assert int(gone) == 0, gone


async def test_runs_list_renders_a_repended_reserved_source_honestly(
    wf_conn: Any, wf_schema: str
) -> None:
    """D8's read-surface arm: a re-pended reserved source (the blob, no
    park marker) is DEFERRING with a next fire from its own due time and
    is NOT counted parked — the pre-fix defect hid it entirely
    (deferring 0, next_fire NULL, parked 0)."""
    from taskq.web.admin.workflows import _RUNS_SQL, _park_predicate

    flow = str(new_uuid())
    meta = _park_meta(flow, width=2, bound=4)
    del meta["blocking_reason"]  # the re-pended reserved source: NOT a park
    meta["wf_deferral"]["attempt"] = 1  # the re-pend's own stamp ordinal
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', "
        "'__flow__', $2::jsonb, 'workflow-run', $3)",
        flow,
        _json.dumps({"flow_id": flow, "workflow": "d8"}),
        f"flow:{flow}",
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'scheduled', 1, 2, 'transient', 'source', "
        "$2::jsonb, clock_timestamp() - interval '2 hours', clock_timestamp(), "
        "$3, 'workflow-run', $4)",
        str(new_uuid()),
        _json.dumps(meta),
        flow,
        f"wf:{flow}:source",
    )
    row = await wf_conn.fetchrow(
        _RUNS_SQL.format(schema=wf_schema, park_predicate=_park_predicate()), 1
    )
    assert row is not None, "the run renders"
    assert int(row["deferring"]) == 1, dict(row)
    assert row["next_fire"] is not None, dict(row)
    assert int(row["parked"]) == 0, dict(row)
