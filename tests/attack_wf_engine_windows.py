"""ATTACK: the engine's crash windows and wire shapes.

Three attacks, one file per the engine-window concern:

1. THE TX1→TX2 CRASH WINDOW LOSES THE REDUCER BODY. ``finalize_node`` =
   tx1 (the fenced terminal + fork) then tx2 (the decrement + fire +
   reducer body + outbox). A worker killed after tx1 commits and before
   tx2 starts leaves the decrement undone; the edge ledger still says the
   parent is terminal. The healing arm is ``sweep_join_rederive``: it
   reconciles the cache from the ledger and FIRES the join
   (``SWEEP_FIRE_SQL``, ``fired_by='sweep'``), writing the outbox rows.
   But the sweep's fire arm has NO reducer mechanism — the body runs only
   in ``_fire_and_deliver`` (the finalize's tx2), which is dead by then.
   The fired join's body executes ZERO times: T05's stated boundary ("a
   raising reducer rolls tx2 back and the body RE-RUNS on re-fire —
   at-least-once body execution", ``ledger.py:38-42``) is violated at the
   window between the engine's own two transactions. Consumers dispatch
   off an un-reduced join.

2. THE EDGE-LESS JOIN NODE IS INVISIBLE TO THE SWEEP. ``insert_node`` is
   the public enqueue path for joined nodes (``deps_pending > 0``), but
   the bundle's ONLY wf_edge writer is the fork's internal one — no edge
   statement is exported, so a joined node inserted through the public
   path has NO edge rows. The rederive arm's ``counts`` CTE INNER-joins
   wf_edge: such a row is neither reconciled, nor fired, nor stamped
   blocked-with-reason (``missing_parents`` requires an edge row to
   exist) — silently stranded in join-wait forever, with a healthy-looking
   record (pin 8's 'record healthy, work wrong' class, un-stamped).

3. THE DRAIN'S CONSUMER PAYLOAD IS THE TRANSPORT ENVELOPE. ``drain_outbox``
   binds the outbox row's WHOLE bindings envelope — ``{"actor":…,
   "queue":…, "payload":…}`` — into the ``$4::jsonb[]`` PAYLOAD slot
   (``_sweep.py:148``), so the consumer body receives the envelope, not
   the declared payload; and the fork's trace_id is dropped
   (``trace_id=[None]*len(rows)``).
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import engine as engine_mod
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import drain_outbox, sweep_join_rederive
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import claim_view, seed_edge, seed_flow, seed_join, seed_running_node


@pytest.mark.integration
async def test_attack_sweep_fire_skips_the_reducer_body(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)

    body_calls = 0

    async def reducer_body() -> None:
        nonlocal body_calls
        body_calls += 1

    # THE CRASH: tx1 commits (the parent terminalizes — the fenced UPDATE
    # returned its row), then the worker dies before tx2. Simulated at the
    # exact statement boundary: _run_tx2 never returns.
    async def dead_tx2(*args: object, **kwargs: object) -> object:
        raise RuntimeError("worker killed between tx1 and tx2")

    monkeypatch.setattr(engine_mod, "_run_tx2", dead_tx2)
    # The raise IS the kill: the worker process dies here (tx1 already
    # committed — nothing rolls it back).
    with pytest.raises(RuntimeError, match="worker killed between tx1 and tx2"):
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=parent,
            step_key="a",
            worker_id=(await claim_view(wf_conn, wf_schema, parent))[0],
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
            reducers={"join": reducer_body},
        )
    monkeypatch.undo()

    # THE HEALING PASS: the sweep reconciles the cache from the edge
    # ledger and fires the join (fired_by='sweep').
    sweep = await sweep_join_rederive(module_pg_pool, wf_sql)
    assert sweep.fired, f"the sweep must fire the crash-window join (got {sweep})"

    # THE CONTRACT: at-least-once body execution. The shipped sweep fire
    # arm has no reducer mechanism — the body ran ZERO times.
    assert body_calls >= 1, (
        f"the join fired via the sweep's healing pass and the reducer body "
        f"ran {body_calls} times: the sweep's fire arm cannot run bodies, so "
        "the tx1→tx2 crash window turns 'at-least-once body execution' into "
        "NEVER — downstream consumers dispatch off an un-reduced join"
    )


@pytest.mark.integration
async def test_attack_join_node_without_edges_is_invisible_to_the_sweep(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    from taskq.workflows._types import NodeSpec

    flow_id = await seed_flow(wf_conn, wf_schema)
    spec: NodeSpec = NodeSpec(
        flow_id=flow_id,
        step_key="orphan",
        actor="wf",
        queue="default",
        deps_pending=1,
    )
    orphan_join = await engine_mod.insert_node(wf_conn, wf_sql, spec)

    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    rec = await wf_conn.fetchrow(  # Why: the fixture's throwaway schema identifier, _IDENT_RE-validated; all values $n-bound.
        f'SELECT status, deps_pending, metadata FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the module fixture's throwaway schema identifier, _IDENT_RE-validated; all values $n-bound.
        orphan_join,
    )
    assert rec is not None
    metadata: dict[str, Any] = (
        rec["metadata"]
        if isinstance(rec["metadata"], dict)
        else json.loads(rec["metadata"] or "{}")
    )

    # A correct engine leaves this row in ONE of the named states: fired
    # (deps 0), or blocked WITH A REASON. The shipped sweep leaves it in
    # none — the row is not even enumerated (the counts CTE inner-joins
    # wf_edge), so the assert's left arm never materializes and deps stays 1.
    assert metadata.get("blocking_reason") == "orphan_parent" or rec["deps_pending"] == 0, (
        f"the edge-less join-wait row is INVISIBLE to the sweep: not blocked "
        f"with a reason ({metadata}), not reconciled (deps_pending="
        f"{rec['deps_pending']}), never firable (no edges to count) — "
        f"silently stranded in join-wait forever (summary={summary})"
    )


@pytest.mark.integration
async def test_attack_drain_consumer_payload_is_the_envelope(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(
        wf_conn,
        wf_schema,
        flow_id,
        consumers=[
            {"step_key": "consumer", "actor": "wf", "queue": "default", "payload": {"x": 1}}
        ],
    )
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)
    worker = new_uuid()
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET locked_by_worker = $2, lock_expires_at = '  # noqa: S608  # Why: the module fixture's throwaway schema identifier, _IDENT_RE-validated; all values $n-bound.
        "now() + interval '90 seconds' WHERE id = $1",
        parent,
        worker,
    )
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent,
        step_key="a",
        worker_id=worker,
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
    )
    assert result.fired
    assert await drain_outbox(module_pg_pool, wf_sql) == 1

    consumer = await wf_conn.fetchrow(
        f'SELECT payload, trace_id FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the module fixture's throwaway schema identifier, _IDENT_RE-validated; all values $n-bound.
        "WHERE step_key = 'consumer' ORDER BY id DESC LIMIT 1"
    )
    assert consumer is not None
    payload: Any = consumer["payload"]
    payload = json.loads(payload) if isinstance(payload, str) else payload
    assert payload == {"x": 1}, (
        f"the drained consumer's payload is {payload!r} — the transport "
        "envelope (actor/queue/payload keys), not the declared payload "
        '{"x": 1}: every drained consumer body must unwrap its own envelope'
    )
    assert consumer["trace_id"] is not None, (
        "the drained consumer's trace_id is NULL — the fork's trace never "
        "reaches the outbox's consumer rows (§18.2's stamp-at-enqueue rule)"
    )
