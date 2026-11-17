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
from taskq.backend._protocol import JobId
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
    RUN'S OWN retention pass (the step-ledger retention's flows-side
    statement, the same statement and transaction that delete the run's
    ledger rows) deletes the record with the run; and nothing removes it
    while the run's own rows still exist."""
    from datetime import timedelta

    from taskq.workflows._sweep import prune_wf_step_ledger

    wsql = _wsql(wf_schema)
    flow = await _seed_marked_flow(wf_conn, wf_schema, with_intent=True)

    # The run's root is RUNNING (not terminal-aged): the retention pass
    # removes nothing.
    await prune_wf_step_ledger(wf_pool, wsql, retention=timedelta(hours=1), batch_size=100)
    still = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow,
    )
    assert int(still) == 1, still

    # THE RUN TERMINAL AND AGED PAST ITS RETENTION: the same pass that
    # deletes the run's ledger rows deletes the run's capacity record.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "finished_at = clock_timestamp() - interval '2 hours' "
        "WHERE id = $1::uuid AND step_key = '__flow__'",
        flow,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "finished_at = clock_timestamp() - interval '2 hours' "
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key <> '__flow__'",
        flow,
    )
    await prune_wf_step_ledger(wf_pool, wsql, retention=timedelta(hours=1), batch_size=100)
    # THE D10 ASSERT: the run's own capacity record went with the run.
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


async def test_vanilla_deadline_release_wake_is_never_lost_by_the_marker_sweep(
    wf_conn: Any, wf_schema: str, wf_pool: Any
) -> None:
    """N3's executed counterexample, as a control: a flow-unaware vanilla
    terminal (the real deadline sweep) frees a live row so a parked
    waiter NOW FITS — the exact-marker sweep is the completeness
    authority for that case and must bump the intent within its own
    selection (the pre-fix selection excluded every capacity-bearing
    flow and the wake was lost forever)."""
    from taskq.workflows._emit import _SWEEP_CURSORS, sweep_emit_markers

    wsql = _wsql(wf_schema)
    # The estate: SIX live peers + one park W=5 under bound 10 — the park
    # does NOT fit (6 + 5 > 10); the intent is NOT pending (w=0, h=0).
    flow = await _seed_marked_flow(wf_conn, wf_schema, width=5, bound=10, with_intent=True)
    for i in range(6):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, '
            "attempt, max_attempts, retry_kind, step_key, map_index, metadata, "
            "created_at, fairness_key, schedule_to_close, idempotency_scope, "
            "idempotency_key) "
            f"VALUES ($1, 'wf', 'default', '{{}}', 'pending', 0, 2, 'transient', "
            "'peer', $2, $3::jsonb, clock_timestamp(), $4, "
            "now() + interval '1 hour', 'workflow-run', $5)",
            str(new_uuid()),
            i,
            _json.dumps({"flow_id": flow}),
            flow,
            f"wf:{flow}:peer{i}",
        )
    # THE PRE-CHECK: the sweep alone bumps nothing (the waiter doesn't fit).
    _SWEEP_CURSORS.pop(wf_schema, None)
    await sweep_emit_markers(wf_pool, wsql)
    intent_before = await wf_conn.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{wf_schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        flow,
    )
    assert intent_before is not None
    assert int(intent_before["wake_generation"]) == 0, intent_before

    # THE VANILLA WRITER'S OWN RELEASE: the real deadline sweep
    # terminalizes one live peer (its schedule_to_close elapsed) — the
    # parked waiter NOW FITS (5 + 5 <= 10).
    await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs
            SET schedule_to_close = now() - interval '1 hour'
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'peer'
              AND map_index = 0""",
        flow,
    )
    from taskq.backend._sweeps import sweep_deadline_exceeded

    swept = await sweep_deadline_exceeded(wf_conn, schema=wf_schema, batch_size=10)
    assert swept == 1, swept

    # THE COMPLETENESS AUTHORITY: the marker sweep re-observes the marked
    # flow and BUMPS the intent — the wake is not lost.
    _SWEEP_CURSORS.pop(wf_schema, None)
    repaired = await sweep_emit_markers(wf_pool, wsql)
    intent_after = await wf_conn.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{wf_schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        flow,
    )
    assert intent_after is not None
    assert int(intent_after["wake_generation"]) == 1, intent_after
    assert int(intent_after["handled_generation"]) < int(intent_after["wake_generation"])
    assert repaired >= 1, repaired


async def test_handled_intent_never_rebumps_while_pending_no_busy_loop(
    wf_conn: Any, wf_schema: str, wf_pool: Any, monkeypatch: Any
) -> None:
    """The no-busy-loop law over the N3 selection: a marked flow whose
    intent is PENDING (the drain's own to observe) is never selected, and
    a handled-but-not-fitting flow is re-observed at the maintenance
    interval WITHOUT a re-bump — two consecutive sweeps each perform
    EXACTLY ONE real observation of the handled flow (the REAL
    `observe_emit_capacity`, counted by flow) and zero for the pending
    flow; the generations never move."""
    import taskq.workflows._emit as emit_mod
    from taskq.workflows._emit import _SWEEP_CURSORS, sweep_emit_markers

    wsql = _wsql(wf_schema)

    # THE REAL OBSERVATION, counted per flow: the delegate calls the real
    # function (the SQL is never replaced) and records which flow was
    # observed.
    real_observe = emit_mod.observe_emit_capacity
    observations: dict[str, int] = {}

    async def _counting_observe(conn: Any, w: Any, *, flow_id: Any, **kwargs: Any) -> Any:
        observations[str(flow_id)] = observations.get(str(flow_id), 0) + 1
        return await real_observe(conn, w, flow_id=flow_id, **kwargs)

    monkeypatch.setattr(emit_mod, "observe_emit_capacity", _counting_observe)

    # The PENDING case: the intent's wake > handled — the drain's own;
    # the sweep never selects it (ZERO marker-repair observations, the
    # generations stay (1, 0)).
    pending_flow = await _seed_marked_flow(wf_conn, wf_schema, width=5, bound=10, with_intent=True)
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_emit_capacity SET wake_generation = 1 WHERE flow_id = $1::uuid',
        pending_flow,
    )
    _SWEEP_CURSORS.pop(wf_schema, None)
    repaired = await sweep_emit_markers(wf_pool, wsql)
    assert repaired == 0, repaired
    assert observations.get(pending_flow, 0) == 0, observations
    intent = await wf_conn.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{wf_schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        pending_flow,
    )
    assert intent is not None and (
        int(intent["wake_generation"]),
        int(intent["handled_generation"]),
    ) == (
        1,
        0,
    ), intent

    # The HANDLED-but-not-fitting case: six live peers hold the charge
    # (6 + 5 > 10 — the parked waiter does NOT fit); the flow is
    # re-observed (the maintenance interval's own work) and NEVER
    # re-bumped.
    handled_flow = await _seed_marked_flow(wf_conn, wf_schema, width=5, bound=10, with_intent=True)
    for i in range(6):
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, '
            "attempt, max_attempts, retry_kind, step_key, map_index, metadata, "
            "created_at, fairness_key, idempotency_scope, idempotency_key) "
            f"VALUES ($1, 'wf', 'default', '{{}}', 'pending', 0, 2, 'transient', "
            "'peer', $2, $3::jsonb, clock_timestamp(), $4, 'workflow-run', $5)",
            str(new_uuid()),
            i,
            _json.dumps({"flow_id": handled_flow}),
            handled_flow,
            f"wf:{handled_flow}:peer{i}",
        )
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_emit_capacity SET wake_generation = 1, '
        "handled_generation = 1 WHERE flow_id = $1::uuid",
        handled_flow,
    )
    _SWEEP_CURSORS.pop(wf_schema, None)
    for _ in range(2):
        await sweep_emit_markers(wf_pool, wsql)
    # EXACTLY ONE real observation per sweep, across the two sweeps.
    assert observations.get(handled_flow, 0) == 2, observations
    intent2 = await wf_conn.fetchrow(
        f'SELECT wake_generation, handled_generation FROM "{wf_schema}".wf_emit_capacity '
        "WHERE flow_id = $1::uuid",
        handled_flow,
    )
    assert intent2 is not None and (
        int(intent2["wake_generation"]),
        int(intent2["handled_generation"]),
    ) == (1, 1), intent2
    # The selection, not another test, makes these pass: the pending flow
    # was never selected even though it was marked.
    assert observations.get(pending_flow, 0) == 0, observations
    _SWEEP_CURSORS.pop(wf_schema, None)


async def test_retention_never_removes_the_record_while_a_live_node_references_the_run(
    wf_conn: Any, wf_schema: str, wf_pool: Any
) -> None:
    """N5: the retention's own gate — a terminal-aged root with a LIVE
    (pending/scheduled/running) node still referencing the run keeps its
    capacity record (the record is the live node's own drain authority;
    removing it strand the node's own park/wake cycle). The record goes
    only when no live node references the run."""
    from datetime import timedelta

    from taskq.workflows._sweep import prune_wf_step_ledger

    wsql = _wsql(wf_schema)
    flow = await _seed_marked_flow(wf_conn, wf_schema, width=5, bound=10, with_intent=True)

    # THE LIVE-NODE SHAPE: the root terminal-aged; ONE node still LIVE
    # (a pending node — the N1 strand's own shape is exactly such a live
    # node). The retention must keep the record.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "finished_at = clock_timestamp() - interval '2 hours' "
        "WHERE id = $1::uuid AND step_key = '__flow__'",
        flow,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "finished_at = clock_timestamp() - interval '2 hours' "
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key <> '__flow__' "
        "AND map_index <> 7",
        flow,
    )
    live_node = JobId(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, map_index, metadata, created_at, "
        "fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 'pending', 0, 2, 'transient', 'peer', "
        "7, $2::jsonb, clock_timestamp(), $3, 'workflow-run', $4)",
        live_node,
        _json.dumps({"flow_id": flow}),
        flow,
        f"wf:{flow}:peer7",
    )
    await prune_wf_step_ledger(wf_pool, wsql, retention=timedelta(hours=1), batch_size=100)
    still = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow,
    )
    assert int(still) == 1, (
        "the retention removed the capacity record while a live node "
        "references the run (N5's own forbidden shape)"
    )

    # THE LIVE WORK RESOLVES (the §5.4 fence's own shape: the parked
    # source under the terminal root is fenced; every node terminal):
    # only now does no live node reference the run.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', "
        "finished_at = clock_timestamp() - interval '2 hours', "
        "metadata = metadata - 'blocking_reason' - 'wf_emit_reservation' "
        '|| \'{"wf_not_draining": {"reason": "terminal_root_after_child_failure_exhaustion"}}\'::jsonb '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid "
        "AND status IN ('pending', 'scheduled', 'running')",
        flow,
    )
    await prune_wf_step_ledger(wf_pool, wsql, retention=timedelta(hours=1), batch_size=100)
    diag = await wf_conn.fetch(
        f'SELECT step_key, status::text AS status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid",
        flow,
    )
    print("N5-DIAG states:", [(r["step_key"], r["status"]) for r in diag], flush=True)
    gone = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_emit_capacity WHERE flow_id = $1::uuid',
        flow,
    )
    assert int(gone) == 0, gone
