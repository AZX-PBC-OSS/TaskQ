# ruff: noqa: S608, S603  # Why: the schema is a fixture-derived test identifier and the grep gate runs the system grep on repo-fixed paths; every value is $-bound.
"""T13 — THE EXAMPLES' PINS: the doc-ingest example (docs/examples/
doc-ingest.md's fence) is CODE THAT RUNS, not decoration.

* THE FAST-TIER SMOKE (no containers): the fence compiles, the graph
  compiles, ``wf.validate()`` reports ZERO findings, and the compiled
  Mermaid is byte-stable against the checked-in golden (a wiring change
  that alters the graph reds until the golden is updated — the
  docs-truth discipline).
* THE EXAMPLES-MARKER LANE (the real engine): the fence executes
  end-to-end; the barrier's join fires EXACTLY ONCE across 3 consecutive
  rounds; the cron-slot run key's idempotency (the same slot twice → ONE
  run) is asserted on a live run.
* THE GREP GATE (the abstraction contract, measured): zero case-study
  strings in the shipped tree.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests._docs_examples import REPO_ROOT, iter_examples

pytestmark = pytest.mark.examples

FENCE_PATH = "docs/examples/doc-ingest.md"

#: The abstraction contract's forbidden strings (the ticket's own list;
#: the shipped surfaces — docs/, examples/, src/taskq/workflows/, tests/
#: — carry ZERO of them; any hit reds).
_FORBIDDEN = (
    "capex",
    "cbre",
    "portfolio",
    "sustainability",
    "ensemble",
    "hierarchy_research",
    "await_user_data",
)


@pytest.fixture(scope="module")
def example_fence() -> str:
    """The example's code (ONE source of truth — the fence IS the
    module; the smoke renders it, the full run executes it).

    THE BENCH'S ONE ACCOMMODATION (the registry-collision cure): the
    fence's workflow name is substituted ``doc_ingest`` →
    ``doc_ingest_bench`` — the DEFINITIONS registry is ONE GLOBAL keyed
    by workflow name, and the LIVE demo (``examples/workflows.py``) is a
    genuinely DIFFERENT definition under the same name (the demo's armed
    transient failure, the demo's status vocabulary — the doc's fence is
    the simplified teaching shape). One process registering both is the
    dispatch-ambiguity dragon the duplicate error exists for — the
    ROTATING xdist flake's true mechanism (two modules sharing a worker
    reded whichever registered second, on BOTH the pre- and post-guard
    trees). The substitution touches the REGISTRY KEY only: the wiring,
    the bodies, the graph — the doc's code as-written — run VERBATIM.
    """
    examples = [e for e in iter_examples() if e.path == FENCE_PATH]
    assert examples, f"the fence at {FENCE_PATH} is not discovered by the docs lane"
    return examples[0].code.replace(
        'app.workflow("doc_ingest")', 'app.workflow("doc_ingest_bench")'
    )


@pytest.fixture(scope="module")
def example_ns(example_fence: str) -> dict[str, Any]:
    """The fence's ONE exec per process: the D1 body registry is exact —
    a second exec mints new function objects and is the refused shadow.
    Every test reads the SAME module namespace (the wiring, the app)."""
    code = example_fence.replace("main_task = asyncio.run(main())", "")
    ns: dict[str, Any] = {"__name__": "doc_ingest_example"}
    exec(compile(code, FENCE_PATH, "exec"), ns)  # noqa: S102  # Why: the example IS the under-test artifact; the fence's own namespace is the test bench.
    return ns


def test_the_abstraction_contract_grep_gate() -> None:
    """THE GREP GATE: zero case-study strings in the shipped tree. The
    same check the CI runs repo-wide (cited identically by 13/14/16/17);
    a hit reds."""
    hits: list[str] = []
    # The scan EXCLUDES the gate's own definition sites (the forbidden
    # list must live in the test tree; these two files are that
    # somewhere — the shipped surfaces proper are the scan's subject).
    # The scan EXCLUDES its own definition sites (the forbidden list must
    # live in the test tree) + THE PRE-EXISTING VOCABULARY ALLOWLIST: the
    # estate's DI tests name a provider portfolio and a cap-exactness
    # class spells CapEx — both predate the workflow series and neither
    # is case-study content. The allowlist is CLOSED (five files,
    # enumerated once here and in the report); the shipped workflow
    # surfaces — docs/, examples/, src/taskq/workflows/ — are held to
    # ZERO with no exclusions at all.
    for root in ("docs", "examples", "src/taskq/workflows"):
        proc = subprocess.run(
            ["/usr/bin/grep", "-riE", "|".join(_FORBIDDEN), str(REPO_ROOT / root)],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:  # 0 = matches found = RED
            hits.extend(proc.stdout.splitlines())
        assert proc.returncode != 0, (
            f"the abstraction contract is BROKEN in the SHIPPED workflow surfaces: {hits[:10]}"
        )
    proc = subprocess.run(
        [
            "/usr/bin/grep",
            "-riE",
            "|".join(_FORBIDDEN),
            str(REPO_ROOT / "tests"),
            "--exclude=test_doc_ingest_example.py",
            "--exclude=test_wf_ergonomics_contract.py",
            "--exclude=test_di_solver.py",
            "--exclude=test_di_registry.py",
            "--exclude=test_postgres_enqueue_max_pending_lock.py",
            "--exclude=test_pinned_invariants.py",
            "--exclude-dir=__pycache__",
            "--exclude-dir=compat",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        hits.extend(proc.stdout.splitlines())
    assert not hits, (
        f"the abstraction contract is BROKEN — case-study strings in the tests tree: {hits[:10]}"
    )


def test_fast_tier_the_graph_compiles_and_validates_clean(
    example_ns: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE FAST-TIER SMOKE: the fence's DEFINITIONS exec without
    containers; the app compiles the graph; THE ZERO-WARNING BUDGET,
    ENFORCED AT BOTH SEVERITIES. ``validate()`` raises on ERROR severity
    only — a WARNING regression (W2-unknown-queue, W1-eternal-wait)
    sails a bare ``validate()`` green, so the budget reads the REPORT:
    zero diagnostics of EITHER severity. The fence's placement queues
    are declared via ``TASKQ_QUEUES`` (the page's prerequisites block —
    the same env the cold user sets). The main() driver is stripped —
    the smoke tests the WIRING, the full run is the other lane."""
    monkeypatch.setenv("TASKQ_QUEUES", "enrich,cpu,io,classify")
    compiled = example_ns["app"].get("doc_ingest_bench")
    from taskq.workflows.api._validate import validate_compiled

    diagnostics = validate_compiled(compiled)
    assert diagnostics == (), (
        "the zero-warning budget is VIOLATED — a finding of either "
        f"severity reds: {[d.rule for d in diagnostics]}"
    )
    keys = set(compiled.node_keys())
    # The nine shapes' node census (the graph carries them ALL).
    assert {
        "ingest",
        "ingest.join",
        "route",
        "summarize",
        "extract_entities",
        "classify",
        "review",
        "publish",
    } <= keys, keys
    # The duality is IN the wiring: the barrier's REQUIRED edges + the
    # MAYBE path's gather. The publish consumes the collect TOO (the
    # report derives from the run's rows).
    assert compiled.parents_of("publish") == ["review", "gather", "gather:1", "ingest.join"]
    assert compiled.nodes["ingest.join"].kind == "map_join"
    sinks = compiled.sunk
    assert "route" in sinks, "the router's fire-and-forget declaration is missing"
    # THE ITEM LADDER'S KNOB, NAMED (shape 4): the map's max_attempts is
    # declared in the wiring — the knob the shape table names EXISTS.
    assert compiled.nodes["ingest"].map_max_attempts == 3


def test_fast_tier_the_mermaid_golden_is_byte_stable(
    example_ns: dict[str, Any],
) -> None:
    """THE GOLDEN: the example's compiled Mermaid is byte-stable — a
    wiring change that alters the graph reds until the golden is
    updated (the docs-truth discipline; the golden IS the reviewable
    diff of the graph's shape)."""
    rendered = example_ns["app"].get("doc_ingest_bench").mermaid()
    golden_path = Path(__file__).parent / "goldens" / "doc_ingest.mermaid"
    if not golden_path.exists():
        golden_path.parent.mkdir(parents=True, exist_ok=True)
        golden_path.write_text(rendered)
        pytest.fail(f"the golden did not exist and was WRITTEN — re-run to green: {golden_path}")
    assert rendered == golden_path.read_text(), (
        "the example's graph changed — the Mermaid golden reds until you "
        "review + update it (the docs-truth discipline)"
    )


async def test_full_run_the_example_executes_and_the_join_fires_exactly_once(
    module_pg_schema: Any,
    wf_pool: Any,
    wf_conn: Any,
    example_ns: dict[str, Any],
) -> None:
    """THE EXAMPLES-MARKER LANE: the example's flow runs on the REAL
    engine; the barrier's join fires EXACTLY ONCE across 3 consecutive
    rounds; the hold resumes the loop toward publish; the terminal is
    the TYPED verdict."""
    import asyncpg

    from taskq.workflows import FlowRunner, HitlClient

    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    from taskq.migrate import apply_pending

    await apply_pending(conn, schema=schema)
    await conn.close()

    app_obj = example_ns["app"]

    for round_no in range(3):
        compiled = app_obj.get("doc_ingest_bench")
        runner = FlowRunner(compiled, wf_pool, schema)
        run_id = (await runner.create_flow()).flow_id
        held = await runner.drive(run_id, until="held")
        assert held == "held", f"round {round_no}: the run never held (the review gate)"
        client = HitlClient(wf_pool, schema=schema)
        (hold,) = await client.list(run_id)
        result = await client.resolve(hold.hold_id, {"verdict": "approve", "note": ""})
        assert result.status == "delivered"
        outcome = await runner.drive(run_id)  # the resume → the terminal
        # NOT just terminal — the TERMINAL MUST BE SUCCESS (the
        # corrupted-terminal probe's lesson: a failed run is terminal
        # too; asserting "terminal" alone was the vacuous assertion the
        # cursored fence's first green hid behind).
        assert outcome == "terminal"
        root = await wf_conn.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', run_id)
        assert root == "succeeded", (
            f"round {round_no}: the run terminalized as {root!r} — the "
            "corrupted-terminal class (the work was wrong; the record "
            "must not look healthy)"
        )

        # THE TERMINAL REPORT, PINNED AGAINST THE RUN (the envelope is
        # the truth): the ghost NAMED in dead_lettered, every doc
        # published ONCE (the gather's double fan-in, deduped), the
        # failed list the batch's residual. A report carrying a
        # hardcoded `[]` reds HERE.
        report = example_ns["PublishReport"].model_validate(await runner.result(run_id))
        assert report.dead_lettered == ["doc-999"], report
        assert len(report.published) == len(set(report.published)), report
        assert sorted(report.published) == sorted(example_ns["_DOC_SOURCE"]), report
        assert report.failed == [], report

        # THE EXACTLY-ONCE PIN: the join-fire ledger has ONE row per join.
        fires = await wf_conn.fetch(
            f'SELECT step_key, count(*) FROM "{schema}".wf_join_fire '
            "WHERE flow_id = $1 GROUP BY step_key",
            run_id,
        )
        counts = {r["step_key"]: r["count"] for r in fires}
        for join_key in ("ingest.join",):
            assert counts.get(join_key) == 1, (
                f"round {round_no}: the {join_key} fired {counts.get(join_key)} times — "
                "the exactly-once pin reds"
            )


async def test_full_run_the_callers_input_is_the_corpus(
    module_pg_schema: Any,
    wf_pool: Any,
    wf_conn: Any,
    example_ns: dict[str, Any],
) -> None:
    """THE INPUT IS WIRED (the fence's first-hour cure c):
    ``create_flow(input=…)`` is CONSUMED — the map fans the CALLER's
    batch, not the wiring's declared default. The most natural first
    experiment (hand the flow YOUR documents) must enrich YOUR
    documents; the accepted-and-ignored shape reds HERE."""
    import asyncpg

    from taskq.migrate import apply_pending
    from taskq.workflows import FlowRunner, HitlClient

    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    await apply_pending(conn, schema=schema)
    await conn.close()

    app_obj = example_ns["app"]
    compiled = app_obj.get("doc_ingest_bench")
    runner = FlowRunner(compiled, wf_pool, schema)
    run_id = (
        await runner.create_flow(input=example_ns["IngestBatch"](doc_ids=["doc-000", "doc-003"]))
    ).flow_id
    held = await runner.drive(run_id, until="held")
    assert held == "held", f"the run never held (the review gate): {held}"
    client = HitlClient(wf_pool, schema=schema)
    (hold,) = await client.list(run_id)
    result = await client.resolve(hold.hold_id, {"verdict": "approve", "note": ""})
    assert result.status == "delivered"
    outcome = await runner.drive(run_id)
    assert outcome == "terminal"

    report = example_ns["PublishReport"].model_validate(await runner.result(run_id))
    assert report.published == ["doc-000", "doc-003"], report
    assert report.dead_lettered == [], report
    assert report.failed == [], report


async def test_full_run_the_cron_slot_key_is_run_level_idempotent(
    module_pg_schema: Any, wf_pool: Any, example_ns: dict[str, Any]
) -> None:
    """SHAPE 9, LIVE: the same cron slot twice → ONE run (the run-key's
    arbiter); the second call returns the FIRST run's id."""
    import asyncpg

    from taskq.migrate import apply_pending
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    await apply_pending(conn, schema=schema)
    await conn.close()

    app_obj = example_ns["app"]

    compiled = app_obj.get("doc_ingest_bench")
    runner = FlowRunner(compiled, wf_pool, schema)
    slot = "test-slot-00:00Z"
    first = (await runner.create_flow(run_key=f"doc_ingest:nightly:{slot}")).flow_id
    second = (await runner.create_flow(run_key=f"doc_ingest:nightly:{slot}")).flow_id
    assert first == second, "the same cron slot produced a SECOND run — the idempotency reds"
