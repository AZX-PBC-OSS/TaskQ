"""The contract §9 control 2: the hoisted observation's plan grammar, the
1,000-waiter ladder arms' envelopes, and the control-2 fixture's facts.

The derivation record (``tests/data/wf_emit_park_envelope.json``) was
measured and checked in BEFORE this control's first execution (clause
(ii-b)); its pins bind this fixture. Real PostgreSQL; ordinary planner
configuration — never a GUC."""

from __future__ import annotations

import enum
import json
from pathlib import Path
from typing import Any, cast

import pytest

from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration

_DEFERRED_AT = "2026-11-15T09:30:00.123456+00:00"  # the fixed literal
_PARK_COUNT = 1000


def _park_meta(flow: str, page_width: int) -> dict[str, Any]:
    """The complete §2.2 park row's metadata as exact literal values — every
    key and value, NO other metadata key."""
    return {
        "flow_id": flow,
        "blocking_reason": "backpressure",
        "wf_deferral": {
            "signal": "EmitBackpressure",
            "consume_budget": False,
            "attempt": 0,
        },
        "wf_emit_backpressure": {
            "version": 1,
            "page_width": page_width,
            "bound": 100,
            "outstanding": 60,
            "deferrals": 1,
            "wakes": 0,
            "deferred_at": _DEFERRED_AT,
        },
        "wf_source_logical_attempt": 1,
    }


def _walk(node: dict[str, Any], acc: list[dict[str, Any]]) -> list[dict[str, Any]]:
    acc.append(node)
    for child in node.get("Plans", []) or []:
        _walk(cast("dict[str, object]", child), acc)
    return acc


def _buffers(document: list[dict[str, Any]]) -> tuple[int, int]:
    """The Plan-tree execution-node buffer sum (the recorded convention:
    sum the per-node fields as reported, which are inclusive of children)
    and the excluded top-level Planning block's own buffers."""
    plan = document[0]["Plan"]
    nodes = _walk(plan, [])
    execution = sum(
        int(n.get("Shared Hit Blocks", 0)) + int(n.get("Shared Read Blocks", 0)) for n in nodes
    )
    planning_block = document[0].get("Planning", {})
    planning = int(planning_block.get("Shared Hit Blocks", 0)) + int(
        planning_block.get("Shared Read Blocks", 0)
    )
    return execution, planning


class _ParkArm:
    """One ladder arm's fixture: the truncated relation, the pinned seed
    order, ANALYZE, and the observed plan."""

    def __init__(self, conn: Any, wsql: WorkflowSql, schema: str, flow_id: str) -> None:
        self._conn = conn
        self._wsql = wsql
        self._schema = schema
        self._flow_id = flow_id
        self.ids = [str(new_uuid()) for _ in range(_PARK_COUNT)]

    async def reset(self) -> None:
        await self._conn.execute(
            f'TRUNCATE "{self._schema}".jobs, "{self._schema}".job_attempts, '
            f'"{self._schema}".job_events CASCADE'
        )

    async def seed(
        self, rows: list[tuple[str, int]], *, step_keys: list[str] | None = None
    ) -> None:
        """The pinned seed order; `step_keys` maps every row to its own
        DECLARED chain source's step key (the declaration→durable-row
        mapping rides the SEED — never an UPDATE pass, which would widen
        the rows after ANALYZE and destroy the pinned layout)."""
        for index, (job_id, width) in enumerate(rows):
            step_key = step_keys[index] if step_keys else f"src_{index:04d}"
            await self._conn.execute(
                f'INSERT INTO "{self._schema}".jobs '
                "(id, actor, queue, payload, status, max_attempts, retry_kind, "
                "step_key, metadata, scheduled_at, created_at, fairness_key) "
                "VALUES ($1, 'a', 'q', '{}', 'scheduled', 3, 'transient', $2, "
                "$3::jsonb, clock_timestamp(), clock_timestamp(), $4)",
                uuid7_of(job_id),
                step_key,
                json.dumps(_park_meta(self._flow_id, width)),
                self._flow_id,
            )
        await self._conn.execute(f'ANALYZE "{self._schema}".jobs')

    async def seed_interleaved(self, rows: list[tuple[str, int]]) -> None:
        """The SAME logical rows (the created_at order is the list order)
        inserted in a physically INTERLEAVED order (newest first, then
        alternating ends) — the park-index page locality destroyed, the
        detection-power arm."""
        count = len(rows)
        insert_order: list[int] = []
        lo, hi = 0, count - 1
        while lo <= hi:
            insert_order.append(hi)
            if hi != lo:
                insert_order.append(lo)
            lo += 1
            hi -= 1
        for index in insert_order:
            job_id, width = rows[index]
            age = count - index  # the logical order: later index = newer
            await self._conn.execute(
                f'INSERT INTO "{self._schema}".jobs '
                "(id, actor, queue, payload, status, max_attempts, retry_kind, "
                "step_key, metadata, scheduled_at, created_at, fairness_key) "
                "VALUES ($1, 'a', 'q', '{}', 'scheduled', 3, 'transient', $2, "
                "$3::jsonb, clock_timestamp(), clock_timestamp() - ($4::text || ' "
                "seconds')::interval, $5)",
                uuid7_of(job_id),
                f"src_{index:04d}",
                json.dumps(_park_meta(self._flow_id, width)),
                str(age * 2),
                self._flow_id,
            )
        await self._conn.execute(f'ANALYZE "{self._schema}".jobs')

    async def run_candidate(self, charge: int, bound: int) -> dict[str, Any]:
        candidate = self._wsql.emit_candidate.replace("$2::bigint", f"{charge}::bigint").replace(
            "$3::bigint", f"{bound}::bigint"
        )
        rows = await self._conn.fetch(candidate, uuid7_of(self._flow_id))
        raw = await self._conn.fetchval(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {candidate}",
            uuid7_of(self._flow_id),
        )
        document = json.loads(raw) if isinstance(raw, str) else raw
        return {"rows": rows, "document": document}

    async def park_row_per_declared_source(self, step_keys: list[str]) -> list[int]:
        """Exactly one CURRENT exact-park row per DECLARED source (the
        declaration→durable-row mapping's own assert, run BEFORE the plan
        checks): one row per key, each an exact §2.2 park."""
        counts = []
        for key in step_keys:
            n = await self._conn.fetchval(
                f"""SELECT count(*) FROM "{self._schema}".jobs
                    WHERE step_key = $1
                      AND (metadata->>'flow_id')::uuid = $2::uuid
                      AND status = 'scheduled'
                      AND metadata @> '{{"blocking_reason":"backpressure"}}'::jsonb""",
                key,
                self._flow_id,
            )
            counts.append(int(n or 0))
        return counts

    @staticmethod
    def print_arm_plan(label: str, document: dict[str, Any], rows: list[Any]) -> None:
        """The contract's own printing duty: the actual plan node names,
        conditions, buffers, rows, and timings — printed, never buried."""
        import time as _time

        plan = document[0]["Plan"]
        print(
            f"PLAN[{label}] execution_ms={plan.get('Actual Total Time')} "
            f"rows={plan.get('Actual Rows')} loops={plan.get('Actual Loops')}",
            flush=True,
        )
        for node in _walk(plan, []):
            name = node.get("Node Type")
            index_name = node.get("Index Name")
            cond = node.get("Index Cond")
            removed = node.get("Rows Removed by Filter")
            actual_rows = node.get("Actual Rows")
            if index_name or removed is not None:
                print(
                    f"PLAN[{label}] node={name} index={index_name} "
                    f"cond={str(cond)[:160]} rows={actual_rows} removed={removed}",
                    flush=True,
                )
        buffers = sum(
            (node.get("Shared Hit Blocks") or 0) + (node.get("Shared Read Blocks") or 0)
            for node in _walk(plan, [])
        )
        print(f"PLAN[{label}] execution_buffers={buffers} result_rows={len(rows)}", flush=True)
        del _time


def new_uuid() -> Any:
    from taskq._ids import new_uuid

    return new_uuid()


def uuid7_of(value: str) -> Any:
    import uuid as _uuid

    return _uuid.UUID(value)


@pytest.mark.integration
async def test_newest_fit_and_oldest_fit_envelopes_and_declaration_mapping(
    wf_conn: Any,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """The 1,000-waiter ladder: the declaration-to-durable-row mapping
    (S = 1,000 from one compiled declaration wired as build(gather([...]))
    — exactly at MAX_FAN_IN_PER_JOIN, never a 1001st fan-in), then both
    arms' plan/buffer/row assertions under the checked-in derivation
    record."""
    from taskq.workflows import DONE, Chain, Route, Step, WorkflowApp, build
    from taskq.workflows.api._graph import chain_source, gather

    class OkOut(enum.Enum):
        OK = "ok"

    async def screen(ctx: object, item: object) -> OkOut:
        del ctx, item
        return OkOut.OK

    async def source_body(ctx: object) -> None:
        del ctx

    app = WorkflowApp()
    chains = [
        Chain(
            name=f"ladder-{index}",
            start=f"screen-{index}",
            steps={
                f"screen-{index}": Step(body=screen, outcomes=OkOut, route=Route({OkOut.OK: DONE}))
            },
        )
        for index in range(_PARK_COUNT)
    ]

    @app.workflow("park_envelope_ladder", max_in_flight=100)
    def park_envelope_ladder() -> object:
        # 1,000 DISTINCT chain_source declarations wired as
        # build(gather([...])) — the variadic gather's list form; exactly at
        # MAX_FAN_IN_PER_JOIN = 1000, and NO 1001st fan-in parent is added.
        promises = [
            chain_source(chain, source_body, key=f"ladder-src-{index}")
            for index, chain in enumerate(chains)
        ]
        return build(gather(promises))

    compiled = app.get("park_envelope_ladder")
    chain_sources = [
        node for node in compiled.nodes.values() if getattr(node, "kind", None) == "chain_source"
    ]
    assert len(chain_sources) == _PARK_COUNT, (
        f"S must be the declared chain-source count ({_PARK_COUNT}); the "
        f"compiled graph carries {len(chain_sources)}"
    )
    from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN

    assert MAX_FAN_IN_PER_JOIN == 1000
    _ = wf_schema

    # THE DECLARATION→DURABLE-ROW MAPPING: every declared chain source's
    # own step key owns EXACTLY ONE current exact-park row — asserted
    # BEFORE the plan checks (the declaration-based `parked <= S` law's
    # own basis; row-level seeding with an unproven declaration count is
    # not sufficient).
    # THE MAPPING'S OWN KEYS: the DECLARED chain sources' OWN step keys
    # (kind == 'chain_source') — never the whole node table (which also
    # carries the gather join; taking sorted(nodes.keys())[:1000] mapped
    # the join and dropped a real declared source).
    declared_keys = sorted(
        key for key, node in compiled.nodes.items() if getattr(node, "kind", None) == "chain_source"
    )
    assert len(declared_keys) == _PARK_COUNT == 1000, (
        f"the declared chain sources' own keys must be exactly S=1000; got {len(declared_keys)}"
    )

    # The derivation record binds the fixture (checked in BEFORE first run).
    record = json.loads((Path(__file__).parent / "data" / "wf_emit_park_envelope.json").read_text())
    assert record["derivation_facts"]["park_heap_pages"] == 91
    assert record["derivation_facts"]["derived_ceiling"] == 224

    schema = wf_schema
    flow_id = str(new_uuid())
    arm = _ParkArm(wf_conn, wf_sql, schema, flow_id)

    # ── NEWEST-FIT (parks-only, pinned contiguous order) ──
    await arm.reset()
    mapping_keys = sorted(declared_keys)[:_PARK_COUNT]
    await arm.seed(
        [(jid, 100) for jid in arm.ids[:999]] + [(arm.ids[999], 1)],
        step_keys=mapping_keys,
    )
    # THE MAPPING (declared key → its own durable row): exactly one
    # current exact-park row per declared source, BEFORE the plan checks.
    per_source = await arm.park_row_per_declared_source(mapping_keys)
    assert all(n == 1 for n in per_source), (
        "exactly one current exact-park row per declared source required; "
        f"got {per_source[:10]}... (all-ones required)"
    )
    result = await arm.run_candidate(charge=60, bound=100)
    arm.print_arm_plan("newest-fit", result["document"], result["rows"])
    assert len(result["rows"]) == 1
    assert str(result["rows"][0]["id"]) == arm.ids[999], (
        "the newest W=1 park is the oldest FITTING waiter's answer"
    )
    execution, planning = _buffers(result["document"])
    assert planning >= 0
    nodes = _walk(result["document"][0]["Plan"], [])
    park_nodes = [n for n in nodes if n.get("Index Name") == "jobs_wf_emit_park_idx"]
    assert len(park_nodes) == 1, result["document"]
    assert park_nodes[0]["Node Type"] in ("Index Scan", "Index Only Scan", "Bitmap Index Scan")
    removed = int(park_nodes[0].get("Rows Removed by Filter", 0))
    assert removed == 999, (
        f"the newest-fit scan examines the whole parked prefix: 999 removals, got {removed}"
    )
    assert execution <= record["derivation_facts"]["derived_ceiling"], (
        f"the newest-fit envelope: {execution} execution-node buffers over "
        f"the ceiling 224 (checked-in basis: {record['derivation_facts']})"
    )

    # ── OLDEST-FIT (parks-only, pinned contiguous order) ──
    await arm.reset()
    await arm.seed(
        [(arm.ids[0], 1)] + [(jid, 100) for jid in arm.ids[1:]],
        step_keys=mapping_keys,
    )
    per_source = await arm.park_row_per_declared_source(mapping_keys)
    assert all(n == 1 for n in per_source), per_source[:10]
    result = await arm.run_candidate(charge=60, bound=100)
    arm.print_arm_plan("oldest-fit", result["document"], result["rows"])
    assert len(result["rows"]) == 1
    assert str(result["rows"][0]["id"]) == arm.ids[0], "the oldest W=1 park is the answer"
    execution, _planning = _buffers(result["document"])
    nodes = _walk(result["document"][0]["Plan"], [])
    park_nodes = [n for n in nodes if n.get("Index Name") == "jobs_wf_emit_park_idx"]
    assert len(park_nodes) == 1
    assert int(park_nodes[0].get("Rows Removed by Filter", 0)) == 0
    assert execution <= record["derivation_facts"]["derived_ceiling"]

    # ── THE INTERLEAVED RED (the pinned order is part of the fixture) ──
    # The SAME logical rows as the newest-fit arm, physically interleaved.
    await arm.reset()
    interleaved_rows = [(jid, 100) for jid in arm.ids[:999]] + [(arm.ids[999], 1)]
    await arm.seed_interleaved(interleaved_rows)
    result = await arm.run_candidate(charge=60, bound=100)
    assert len(result["rows"]) == 1
    assert str(result["rows"][0]["id"]) == arm.ids[999], "the logical order is unchanged"
    execution, _planning = _buffers(result["document"])
    # The interleaved layout destroys the park-index page locality (the
    # round-7 build measured 1004 against the pinned 200); any legal-plan
    # run over it must exceed the envelope — the detection-power proof.
    assert execution > record["derivation_facts"]["derived_ceiling"], (
        f"the interleaved layout must exceed the envelope (the recorded "
        f"1,004-vs-200 separation); measured {execution}"
    )


@pytest.mark.integration
async def test_hoisted_observation_at_the_control2_shape(
    wf_conn: Any,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """The control-2 fixture: 200 live non-root rows + one exact park W=17 +
    50,000 terminal rows for one flow, NO live root row, category-clustered
    seed order (terminal first, then live, then the park), ANALYZE. The
    hoisted observation returns byte-identical facts (200, 0, 17), examines
    exactly 201 rows, removes 0 (no root), stays at or under the 64-buffer
    ceiling, and the unhoisted mutant examines 50,201 rows over the
    envelope."""
    from taskq._ids import new_uuid

    flow_id = str(new_uuid())
    schema = wf_schema

    await wf_conn.execute(
        f'TRUNCATE "{schema}".jobs, "{schema}".job_attempts, "{schema}".job_events CASCADE'
    )
    # 1. the 50,000 terminal rows first (one contiguous category insert).
    terminal_rows = []
    for _ in range(50_000):
        terminal_rows.append((str(new_uuid()), json.dumps({"flow_id": flow_id})))
    await wf_conn.executemany(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "step_key, metadata, fairness_key) "
        "VALUES ($1, 'a', 'q', '{}', 'succeeded', 3, 'transient', 'done', "
        "$2::jsonb, $3)",
        [(uuid7_of(jid), meta, flow_id) for jid, meta in terminal_rows],
    )
    # 2. the 200 live non-root rows contiguously.
    for _ in range(200):
        await wf_conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, "
            "step_key, metadata, fairness_key) "
            "VALUES ($1, 'a', 'q', '{}', 'pending', 3, 'transient', 'peer', "
            "$2::jsonb, $3)",
            uuid7_of(str(new_uuid())),
            json.dumps({"flow_id": flow_id}),
            flow_id,
        )
    # 3. the one exact park (W=17) last — with the live root ABSENT.
    park_id = str(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "step_key, metadata, scheduled_at, created_at, fairness_key) "
        "VALUES ($1, 'a', 'q', '{}', 'scheduled', 3, 'transient', 'src', "
        "$2::jsonb, clock_timestamp(), clock_timestamp(), $3)",
        uuid7_of(park_id),
        json.dumps(_park_meta(flow_id, 17)),
        flow_id,
    )
    await wf_conn.execute(f'ANALYZE "{schema}".jobs')

    observation = await wf_conn.fetchrow(wf_sql.emit_capacity_observation, uuid7_of(flow_id), None)
    assert observation is not None
    facts = (
        int(observation["live_rows"]),
        int(observation["reservations"]),
        observation["min_waiting_width"],
    )
    assert facts == (200, 0, 17), facts

    raw = await wf_conn.fetchval(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {wf_sql.emit_capacity_observation}",
        uuid7_of(flow_id),
        None,
    )
    document = json.loads(raw) if isinstance(raw, str) else raw
    nodes = _walk(document[0]["Plan"], [])
    execution, planning = _buffers(document)

    # THE DUAL PLAN GRAMMAR (§2.3): exactly one jobs node, in one of the two
    # legal forms — Index Scan on jobs_wf_flow_nodes_idx, or Bitmap Heap
    # Scan on jobs whose sole child is one Bitmap Index Scan on that index.
    jobs_nodes = [n for n in nodes if n.get("Relation Name") == "jobs"]
    assert len(jobs_nodes) == 1, document
    jobs_node = jobs_nodes[0]
    scan_nodes = [n for n in nodes if n.get("Index Name") == "jobs_wf_flow_nodes_idx"]
    assert len(scan_nodes) == 1, document
    legal = (
        jobs_node["Node Type"] == "Index Scan"
        and jobs_node.get("Index Name") == "jobs_wf_flow_nodes_idx"
    ) or (
        jobs_node["Node Type"] == "Bitmap Heap Scan"
        and scan_nodes[0]["Node Type"] == "Bitmap Index Scan"
        and scan_nodes[0].get("Relation Name") == "jobs"
    )
    assert legal, document
    cond = str(scan_nodes[0].get("Index Cond", ""))
    normalized = "".join(cond.split())
    assert (
        "flow_id" in normalized
        and "pending" in normalized
        and "scheduled" in normalized
        and "running" in normalized
    ), cond
    actual_rows = int(scan_nodes[0].get("Actual Rows", 0))
    assert actual_rows == 201, (
        f"the feeding scan examines exactly 201 rows (200 live + 1 park, no "
        f"root): got {actual_rows}"
    )
    removed = int(scan_nodes[0].get("Rows Removed by Filter", 0))
    assert removed == 0, f"no live root exists, so the non-root filter removes 0: got {removed}"
    assert execution <= 64, (
        f"the hoisted observation's envelope at the control-2 shape: "
        f"{execution} execution-node buffers over the 64 ceiling"
    )
    assert planning >= 0

    # THE UNHOISTED MUTANT (the prohibited aggregate): a FILTER-only
    # reference over the same rows examines 50,201 rows and exceeds the
    # envelope.
    unhoisted = (
        f"SELECT count(*) FILTER (WHERE NOT (j.status = 'scheduled' AND j.metadata @> "
        f'\'{{"blocking_reason":"backpressure"}}\'::jsonb)) AS live_rows '
        f"FROM \"{schema}\".jobs AS j WHERE (j.metadata->>'flow_id')::uuid = $1::uuid "
        "AND j.metadata ? 'flow_id' "
        "AND j.step_key IS NOT NULL AND j.step_key <> '__flow__'"
    )
    raw = await wf_conn.fetchval(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {unhoisted}", uuid7_of(flow_id)
    )
    document = json.loads(raw) if isinstance(raw, str) else raw
    nodes = _walk(document[0]["Plan"], [])
    examined = sum(
        int(n.get("Actual Rows", 0)) + int(n.get("Rows Removed by Filter", 0))
        for n in nodes
        if n.get("Relation Name") == "jobs"
    )
    execution, _planning = _buffers(document)
    assert examined >= 50_201, f"the unhoisted reference examines the terminal history: {examined}"
    assert execution > 64, f"the unhoisted mutant exceeds the envelope: {execution}"
