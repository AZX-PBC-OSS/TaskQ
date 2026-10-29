"""ATTACK: the engine's crash windows and wire shapes.

Three attacks, one file per the engine-window concern:

1. THE TX1→TX2 CRASH WINDOW LOSES THE REDUCER BODY — THE CROSS-PROCESS
   FORM. ``finalize_node`` = tx1 (the fenced terminal + fork) then tx2
   (the decrement + fire + reducer body + outbox). A worker killed after
   tx1 commits and before tx2 starts leaves the decrement undone; the
   edge ledger still says the parent is terminal. The healing arm is
   ``sweep_join_rederive``: it reconciles the cache from the ledger and
   FIRES the join (``SWEEP_FIRE_SQL``, ``fired_by='sweep'``), writing the
   outbox rows — and runs the fired join's REDUCER BODY. The body must
   resolve in the HEALER'S process, which is never guaranteed to be the
   finalizer's: the sweep heals schema-wide, ANY leader heals ANY flow's
   join-wait rows. So the resolution is DURABLE — the flow root's
   metadata names its workflow (stamped at ``insert_flow_run``) and the
   fire arm resolves the body FROM THE REGISTERED DEFINITION (the
   definition registry every process carries — D1's
   BODY-FROM-DEFINITION); the process-local reducer memo is a cache,
   never the source. This attack drives the shape END TO END: the
   finalize dies at the window (its memo primed, then dropped — the
   process's memory is gone), and a FRESH interpreter process — no memo,
   no shared state, only the registered definition + the stamped name —
   runs the heal. The body MUST run there: at-least-once body execution
   surviving the process boundary, consumers never dispatching off an
   un-reduced join.

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

import asyncio
import json
import os
import subprocess  # Why: the cross-process attack IS a fresh-interpreter probe (the pin-21 pattern).
import sys
from pathlib import Path
from typing import Any, cast

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows import engine as engine_mod
from taskq.workflows._reducers import forget_flow_reducers
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import drain_outbox, sweep_join_rederive
from taskq.workflows.definitions import StepBody, WorkflowDef, get_registry
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import claim_view, seed_edge, seed_flow, seed_join, seed_running_node

#: The attack definition's registered name — the flow root's stamp and the
#: subprocess's registration must agree (the fleet convention: the same
#: definitions imported in every process).
_WORKFLOW_NAME = "attack-cross-process-reducer"

#: The fresh-process healer: registers the definition (its OWN body
#: closure — nothing shared with the parent), runs the sweep's rederive
#: arm against the stamped flow root, prints the JSON verdict. The parent
#: primes no memo here; the ONLY body the healer can resolve is the
#: registered definition's — the durable leg.
_HEALER_SCRIPT = """\
import asyncio, json, os, sys

import asyncpg

from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.definitions import WorkflowDef, get_registry
from taskq.workflows.engine import render_workflow_sql

schema = sys.argv[1]
calls = {"body": 0}


async def join_body(ctx: object, items: object = None) -> object:
    calls["body"] += 1
    return "reduced"


get_registry().register(WorkflowDef(name="attack-cross-process-reducer", bodies={"join": join_body}))


async def main() -> None:
    pool = await asyncpg.create_pool(os.environ["TASKQ_PG_DSN"])
    try:
        result = await sweep_join_rederive(pool, render_workflow_sql(schema))
        # THE EXECUTION IS THE CLAIM'S (the wedged-hold cure's contract):
        # the fire marks + delivers; the fired row's own claim runs the
        # body with its PROPER convention (the registry's body is a step
        # body — ctx + the parents' list — never a zero-arg reducer). The
        # heal's honest verdict: the row left CLAIMABLE (pending, deps 0,
        # no blocking stamp, no hold) — the claim arbiter owns the
        # at-least-once from here, cross-process by construction (the
        # registry is fleet-wide).
        row = await pool.fetchrow(
            "SELECT status, deps_pending, metadata FROM "
            "{schema}.jobs WHERE step_key = 'join'".replace("{schema}", schema)
        )
        meta = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"]
        # CLAIMABLE = the runner's own fence's shape: pending, the counter
        # at 0, NO hold stamp (the blocking_reason='join' marker is the
        # row's own birth record — every parented node carries it; it is
        # not a wedge).
        claimable = (
            row is not None
            and row["status"] == "pending"
            and row["deps_pending"] == 0
            and not meta.get("hold")
        )
        print(json.dumps({
            "fired": len(result.fired),
            "claimable": claimable,
            "row": None if row is None else {
                "status": row["status"],
                "deps": row["deps_pending"],
                "meta": meta,
            },
        }))
    finally:
        await pool.close()


asyncio.run(main())
"""


@pytest.mark.integration
async def test_attack_sweep_fire_skips_the_reducer_body(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    pg_dsn: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CROSS-PROCESS form: the healer is a FRESH interpreter process —
    no reducer memo, no shared state — and the body must still run,
    resolved from the REGISTERED DEFINITION via the flow root's stamped
    workflow name. The memo-only shape (the finalizing process's memory
    as the source of truth) is the convicted variant: the sweep heals
    schema-wide, so the healer is never guaranteed to be the finalizer —
    the body would run ZERO times fleet-wide and the consumers would
    dispatch off an un-reduced join."""
    flow_id = await seed_flow(wf_conn, wf_schema, workflow=_WORKFLOW_NAME)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)

    # THE DEFINITION: registered under the name the flow root stamps —
    # the convention every worker process follows (the registry content
    # is schema-level by construction: the same definitions imported in
    # every process). The parent's registry copy is NOT the healer's —
    # the heal runs in a fresh process that registers its OWN; the
    # parent's copy exists so the finalize's reducers argument declares a
    # body, and to make the point: the parent's memory is not what runs.
    async def parent_body(ctx: object) -> None:
        return None

    get_registry().register(
        WorkflowDef(name=_WORKFLOW_NAME, bodies={"join": cast("StepBody", parent_body)})
    )

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
            reducers={"join": parent_body},
        )
    monkeypatch.undo()

    # THE PROCESS DEATH: the finalizing process's memory is gone — the
    # memo it warmed dies with it. Nothing in THIS process can answer the
    # heal's resolution anymore.
    forget_flow_reducers(flow_id)
    from taskq.workflows._reducers import resolve_flow_reducer

    resolved = resolve_flow_reducer(flow_id, "join")
    assert resolved.body is None, (
        "the memo still answers after the finalizing process died — the "
        "cross-process attack below would not exercise the durable leg"
    )

    # THE HEALING PASS IN A FRESH PROCESS: a new interpreter — importing
    # taskq.workflows, registering the SAME definition name (the fleet
    # convention), NO reducer memo — runs sweep_join_rederive against the
    # stamped flow root. The verdict is the subprocess's own printout.
    script = tmp_path / "_attack_cross_process_heal.py"
    script.write_text(_HEALER_SCRIPT)
    proc = await asyncio.to_thread(
        subprocess.run,  # Why: fixed argv + tmp_path script, the pin-21 fresh-interpreter pattern.
        [sys.executable, str(script), wf_schema],
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, "TASKQ_PG_DSN": pg_dsn},
    )
    assert proc.returncode == 0, (
        f"the fresh-process healer crashed (rc={proc.returncode}):\n{proc.stdout}\n{proc.stderr}"
    )
    verdict = json.loads(proc.stdout.strip().splitlines()[-1])
    assert verdict["fired"] >= 1, (
        f"the sweep's healing pass did not fire the crash-window join in "
        f"the fresh process (verdict={verdict})"
    )
    # THE CONTRACT: the heal is CROSS-PROCESS COMPLETE — the fresh
    # process fired the join AND left the row CLAIMABLE (pending, deps 0,
    # no stamp): the execution is the CLAIM's own contract (the registry's
    # body is a step body — ctx + the parents' list — run by the row's
    # own claim, the runner's own pins), never the fire's; the fire's
    # exactly-once + the claim's arbiter carry the at-least-once,
    # cross-process by construction (the registry is fleet-wide). The
    # convicted variants: the row stamped body_unavailable (the record
    # lies — the resolution's loud face fired for a REGISTERED name) or
    # the row left unfired (the heal did not heal).
    assert verdict["claimable"] is True, (
        f"the join fired via the sweep's healing pass in the FRESH process "
        f"but the row did not settle claimable: the heal left the record "
        f"wedged (a body_unavailable stamp over a REGISTERED name — the "
        f"resolution's loud face fired where the fleet's own registry "
        f"answers — or the row's counter/hold state moved) "
        f"(verdict={verdict})"
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
