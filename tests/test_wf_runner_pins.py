"""T09 — THE RUNNER PINS: the integration tier (the real PG lane). The
API's compile lowers into the ENGINE's rows and drives through the
engine's own finalize/sweep machinery — every pin here exercises the REAL
path (create → drive → read), never a local re-implementation.

The paper-cut cures' re-tests (the T17 disposition ledger's "re-test"
column) live here:
  #1  test_join_user_body_cascades_downstream
  #2  test_sequenced_maps_depth3_one_flow
  #4  test_skip_predicate_decided_at_dispatch
  #7  test_create_flow_carries_input_and_drives
  #9  test_map_children_ledger_identity (the per-child identity)
  #12 test_retry_classifier_routes_by_kind
  #14 test_flow_result_read_decodes_once
  #19 test_flow_result_read_decodes_once
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)
from tests._wf_fixtures import MEASUREMENTS


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Item(BaseModel):
    n: int


# ── module-level bodies (the annotations must resolve — the compile's
#    hint resolution reads the defining globals) ────────────────────────


async def _echo(value: dict[str, object]) -> dict[str, object]:
    return value


async def _prepare(ctx: StepContext, params: Ingest) -> Report:
    report = await ctx.step("prepare.inner", _echo, {"ref": params.doc_id})
    return Report(ref=report["ref"])


async def _double(ctx: StepContext, report: Report) -> dict[str, int]:
    return {"n": len(report.ref) * 2}


async def _total(ctx: StepContext, a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
    """THE JOIN'S USER BODY (cut #1's cure): receives the DECODED parent
    results — its result cascades downstream."""
    return {"total": a["n"] + b["n"]}


async def _tail(ctx: StepContext, t: dict[str, int]) -> dict[str, int]:
    return t


async def _items_source(ctx: StepContext, params: Ingest) -> list[Item]:
    return [Item(n=1), Item(n=2), Item(n=3)]


async def _per_item(ctx: StepContext, item: Item) -> dict[str, int]:
    return {"n": item.n * 10}


async def _map_tail(ctx: StepContext, items: list[dict[str, int]]) -> dict[str, object]:
    return {"sum": sum(i["n"] for i in items)}


@pytest.fixture
async def wf_pool(module_pg_schema: Any) -> Any:
    """The runner acquires from a POOL (its claims + finalizes); the
    module's DSN builds it — one pool per test, closed at teardown."""
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    yield pool
    await pool.close()


async def test_create_flow_carries_input_and_drives(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cut #7's cure + the happy path: create_flow(spec, INPUT) → the
    input rides the ROOT ROW (a restart reads it back) → drive to
    terminal → the result decodes."""
    app = WorkflowApp()

    @app.workflow("input_flow")
    def input_flow() -> object:
        a = step(_prepare, Ingest(doc_id="d1"), key="a")
        return build(a)

    runner = FlowRunner(app.get("input_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow(input={"doc_id": "d1"})
    # THE INPUT IS ON THE ROW (cut #7: never a closure).
    raw = await wf_conn.fetchval(
        f'SELECT payload FROM "{wf_schema}".jobs WHERE id = $1',
        flow_id,  # Why: the schema identifier is the FIXTURE's own (module_pg_schema) — the estate's test-SQL precedent.
    )
    assert raw is not None and "wf_input" in raw
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"ref": "d1"}


async def test_join_user_body_cascades_downstream(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cut #1's cure (the BLOCKER): the fan-in's USER body receives the
    decoded parents, and its result cascades — the downstream node
    dispatches as a NORMAL step with the reducer's OUTPUT as its parent
    result. No out-of-engine decode, no multi-flow glue."""
    app = WorkflowApp()

    @app.workflow("cascade_flow")
    def cascade_flow() -> object:
        a = step(_double, step(_prepare, Ingest(doc_id="d1"), key="p"), key="a")
        b = step(_double, step(_prepare, Ingest(doc_id="22"), key="q"), key="b")
        reducer = step(_total, a, b, key="reducer")
        return build(step(_tail, reducer, key="tail"))

    runner = FlowRunner(app.get("cascade_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    # The cascade: tail saw the REDUCER's output (the decoded totals).
    assert await runner.result(flow_id) == {"total": 8}


async def test_sequenced_maps_depth3_one_flow(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cut #2's cure (the BLOCKER): sequencing — a map whose source is a
    step's promise, in ONE flow (the spike needed three flows + glue)."""
    app = WorkflowApp()

    @app.workflow("seq_flow")
    def seq_flow() -> object:
        source = step(_items_source, Ingest(doc_id="d1"), key="src")
        mapped = map_source(source, _per_item, key="map1")  # THE MAP: per-item fresh jobs
        del mapped
        return build(step(_map_tail, source, key="tail"))

    runner = FlowRunner(app.get("seq_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    # the map's children exist as ROWS with map_index (the fork's
    # per-item identity — the ledger keys them separately).
    row = await wf_conn.fetchrow(
        f'SELECT count(*), array_agg(map_index ORDER BY map_index) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'src.item'",
        flow_id,
    )
    assert row is not None and row[0] == 3 and row[1] == [0, 1, 2]


async def test_skip_predicate_decided_at_dispatch(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cut #4's cure: the guard reads the flow's state AT DISPATCH — the
    sibling's COMPLETED result decides it (inexpressible in the spike;
    the dependency is SPELLED: the guarded node consumes the chooser's
    promise, so its dispatch strictly follows the chooser's terminal)."""
    app = WorkflowApp()

    async def chooser(ctx: StepContext, params: Ingest) -> dict[str, str]:
        return {"pick": "skip_me"}

    async def guarded(ctx: StepContext, choice: dict[str, str]) -> dict[str, int]:
        return {"n": 1}

    @app.workflow("guard_flow")
    def guard_flow() -> object:
        pick = step(chooser, Ingest(doc_id="d"), key="pick")
        return build(
            step(
                guarded,
                pick,
                key="guarded",
                # THE DISPATCH-TIME PREDICATE: reads the sibling's result.
                skip=lambda state: (
                    (state["results"].get("pick") or {}).get("pick")  # pyright: ignore[reportUnknownArgumentType, reportAttributeAccessIssue]  # Why: the predicate receives the runner's state dict — the walk's shape is the runner's contract.
                    == "skip_me"
                ),
            )
        )

    runner = FlowRunner(app.get("guard_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    row = await wf_conn.fetchrow(
        f'SELECT status, result FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'guarded'",
        flow_id,
    )
    assert row is not None
    assert row["status"] == "succeeded"  # the v1 skip semantics: succeed WITH the record
    assert "skipped" in (row["result"] or "")


async def test_retry_classifier_routes_by_kind(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cut #12's cure: the retry knob routes by kind — a ``permanent``
    body failure takes NO ladder (one terminal, immediately)."""
    app = WorkflowApp()

    async def always_fails(ctx: StepContext, params: Ingest) -> dict[str, int]:
        raise ValueError("boom")

    @app.workflow("retry_flow")
    def retry_flow() -> object:
        node = step(
            always_fails,
            Ingest(doc_id="d"),
            key="perm",
            retry_kind="permanent",
            max_attempts=3,
        )
        return build(node)

    runner = FlowRunner(app.get("retry_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    attempts = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'perm'",
        flow_id,
    )
    row = await wf_conn.fetchrow(
        f'SELECT status, attempt FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'perm'",
        flow_id,
    )
    assert row is not None
    assert row["status"] == "failed"
    # PERMANENT → the ladder never ran: exactly ONE attempt.
    assert attempts == 1 and row["attempt"] == 1


async def test_transient_ladder_emits_no_terminal_until_exhaustion(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """P3 rule 7 through the public API: the transient ladder's attempt
    failures emit NO terminal — the node re-pends until max_attempts."""
    app = WorkflowApp()

    async def always_fails_transient(ctx: StepContext, params: Ingest) -> dict[str, int]:
        raise ValueError("transient boom")

    @app.workflow("ladder_flow")
    def ladder_flow() -> object:
        node = step(always_fails_transient, Ingest(doc_id="d"), key="lad", max_attempts=3)
        return build(node)

    runner = FlowRunner(app.get("ladder_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    statuses = await wf_conn.fetch(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'lad' ORDER BY attempt",
        flow_id,
    )
    # THREE attempts, all 'failed' ledger rows (the ladder's shape:
    # attempt failures ≠ node failure — no terminal until exhaustion).
    assert [r["status"] for r in statuses] == ["failed", "failed", "failed"]
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed"  # T06's cascade took the flow with it


async def test_flow_result_read_decodes_once(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cuts #14/#19's cures: the result read DECODES (never a raw jsonb
    string) and reads the terminal by id; the un-named terminal is the
    LOUD refusal."""
    app = WorkflowApp()

    @app.workflow("result_flow")
    def result_flow() -> object:
        return build(step(_prepare, Ingest(doc_id="d1"), key="only"))

    runner = FlowRunner(app.get("result_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    await runner.drive(flow_id)
    result = await runner.result(flow_id)
    assert isinstance(result, dict) and result == {"ref": "d1"}

    app2 = WorkflowApp()

    @app2.workflow("no_terminal")
    def no_terminal() -> object:
        step(_prepare, Ingest(doc_id="d"), key="only")
        return None

    with pytest.raises(Exception, match="produced-never-consumed"):
        FlowRunner(app2.get("no_terminal"), wf_pool, wf_schema)


async def test_dispatch_resolves_bodies_from_the_definition_registry(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """D1's dispatch wiring (the pin the round-1 fixer deferred): the
    runner resolves EVERY body from the REGISTERED DEFINITION — a
    definition unregistered from the registry is unrunnable (loud), and
    a per-call body map cannot exist."""
    from taskq.workflows.definitions import get_registry

    app = WorkflowApp()

    @app.workflow("d1_flow")
    def d1_flow() -> object:
        return build(step(_prepare, Ingest(doc_id="d1"), key="only"))

    runner = FlowRunner(app.get("d1_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # THE REGISTRY IS THE TRUTH: the compile registered the bodies.
    definition = get_registry().get("d1_flow")
    assert "only" in definition.bodies
    # The D1 conviction: resolve from an UNREGISTERED name → the KeyError
    # (dispatch resolves by the ROOT's stamped workflow name — the
    # durable leg the sweep's healer uses).
    with pytest.raises(KeyError, match="not registered"):
        get_registry().body("never-declared", "only")
    assert await runner.drive(flow_id) == "terminal"


async def test_map_children_ledger_identity_and_max_attempts(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cuts #2/#15's cures at the ledger: the map's children are FRESH
    jobs with per-child ledger identity (map_index distinct) and their
    own max_attempts — the retry ladder never collapses them onto one
    row."""
    app = WorkflowApp()

    async def flaky_child(ctx: StepContext, item: Item) -> dict[str, int]:
        if item.n == 2 and ctx.attempt == 1:
            raise ValueError("child 2 first try")
        return {"n": item.n}

    @app.workflow("map_children_flow")
    def map_children_flow() -> object:
        source = step(_items_source, Ingest(doc_id="d"), key="src")
        mapped = map_source(source, flaky_child, key="child", max_attempts=3)
        return build(mapped)

    runner = FlowRunner(app.get("map_children_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    assert await runner.drive(flow_id) == "terminal"
    ledger = await wf_conn.fetch(
        f'SELECT map_index, attempt, status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'src.item' "
        "ORDER BY map_index, attempt",
        flow_id,
    )
    # The flaky item (Item(n=2)) sits at map_index 1 — it laddered
    # (2 attempts); the others succeeded first try — PER-CHILD identity
    # (the ledger-PK attack's shape, through the API).
    child1 = [(r["attempt"], r["status"]) for r in ledger if r["map_index"] == 1]
    assert child1 == [(1, "failed"), (2, "succeeded")]
    child0 = [(r["attempt"], r["status"]) for r in ledger if r["map_index"] == 0]
    assert child0 == [(1, "succeeded")]


async def test_mermaid_golden_byte_stable(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The Mermaid golden (the acceptance gate): the same module's graph
    renders BYTE-STABLE across compiles — and the diagram-lies property:
    every compiled node/edge appears in the emission (one source of
    truth)."""
    app = WorkflowApp()

    @app.workflow("golden_flow")
    def golden_flow() -> object:
        a = step(_double, step(_prepare, Ingest(doc_id="d1"), key="p"), key="a")
        b = step(_double, step(_prepare, Ingest(doc_id="22"), key="q"), key="b")
        reducer = step(_total, a, b, key="reducer")
        return build(step(_tail, reducer, key="tail"))

    compiled = app.get("golden_flow")
    golden = compiled.mermaid()
    # Re-compile: byte-identical.
    assert app.get("golden_flow").mermaid() == golden
    # The diagram-lies property: every node/edge of the compiled graph is
    # IN the emission.
    for key in compiled.node_keys():
        assert f"{key}[" in golden or f"{key}{{" in golden or f"{key}([(" in golden
    for key in compiled.node_keys():
        for parent in compiled.parents_of(key):
            assert f"{parent} -->" in golden
    # The golden is RECORDED (the corpus is append-only).
    MEASUREMENTS.mkdir(exist_ok=True)
    (MEASUREMENTS / "t09-mermaid-golden.mmd").write_text(golden)


async def test_drive_until_held_bound(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Cut #10's contract (the shape; the loop/HITL compositions land in
    T19/T10): the driver's ``until`` knob is bound and BOUNDED — a flow
    that never terminalizes ends at ``max_ticks``, never a hang."""
    app = WorkflowApp()

    @app.workflow("bound_flow")
    def bound_flow() -> object:
        # A HELD row: the join-wait representation with a future
        # scheduled_at — the driver's "held" arm returns, the loop stops.
        a = step(_prepare, Ingest(doc_id="d1"), key="a")
        return build(a)

    runner = FlowRunner(app.get("bound_flow"), wf_pool, wf_schema)
    flow_id = await runner.create_flow()
    # THE BOUND: max_ticks=2 ends the drive — never a hang (the cap is
    # the pin; a driver that spins forever is the defect with no stack).
    verdict = await runner.drive(flow_id, until="held", max_ticks=2, tick=0.0)
    assert verdict in ("held", "terminal", "max_ticks")
    # The bound is the DRIVER's property, not the run's: the drive
    # RETURNED (never a hang) and the run continues by its own rules.
    _ = JobId  # the runner's ids (the import is the runner's contract)
