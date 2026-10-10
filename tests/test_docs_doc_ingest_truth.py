# ruff: noqa: S608, S102  # Why: the schema is a fixture-derived test identifier and the doc fence IS the under-test artifact (exec'd — the docs lane's own pattern, test_doc_ingest_example.py).
"""DOC-TRUTH PINS — F-DEMO-4: the doc-ingest fence (docs/examples/
doc-ingest.md) is CODE THAT RUNS, and its OUTPUT claims are pinned where
they are executable (the hostile review of af1b8779, demo lane).

The fence ships four doc-vs-code divergences; three are executable
claims and pin here, strict-xfail until the fence is cured (the cure
flips the pin to XPASS-strict — remove the marker WITH the cure):

* (c) ``create_flow(input=…)`` is ACCEPTED but IGNORED — the ingest
  step's params are baked into the wiring (``IngestBatch(doc_ids=
  sorted(_DOC_SOURCE))``), so the run row's input never reaches the
  body. THE PIN: a one-doc input fans a one-doc map.
* (d) the terminal ``PublishReport`` hardcodes ``dead_lettered=[]`` /
  ``failed=[]`` while the doc claims "the failures ride the report,
  named". THE PIN: a dead-lettered doc is NAMED in the terminal report
  and never rides the published list.
* (e) the published list DOUBLE-COUNTS every doc (the barrier's gather
  concatenates [summaries + entities]). THE PIN: each doc is named once.

The fourth, (b) — the fence's ``drive(until="terminal")`` spins ~166 s
PAST the hold, the ``max_ticks`` verdict silently discarded — pins as a
static doc-truth assertion (the fence must drive ``until="held"`` before
the resolve). (a) — the bare ``os.environ[...]`` KeyError on the
undocumented env vars — is a fence-text defect with no feasible
assertion shape; it lives in DEFECTS.md with the exact cure text.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest

from tests._docs_examples import iter_examples

pytestmark = pytest.mark.examples

FENCE_PATH = "docs/examples/doc-ingest.md"


@pytest.fixture(scope="module")
def example_fence() -> str:
    """The fence's code, renamed off the demo's registry key (the same
    accommodation test_doc_ingest_example.py documents: the D1 registry
    is ONE GLOBAL keyed by workflow name; the wiring, the bodies, the
    graph run VERBATIM)."""
    examples = [e for e in iter_examples() if e.path == FENCE_PATH]
    assert examples, f"the fence at {FENCE_PATH} is not discovered by the docs lane"
    return examples[0].code.replace(
        'app.workflow("doc_ingest")', 'app.workflow("doc_ingest_truth")'
    )


@pytest.fixture(scope="module")
def example_ns(example_fence: str) -> dict[str, Any]:
    """The fence's ONE exec per process (the D1 body registry is exact —
    a second exec mints new function objects and is the refused shadow)."""
    code = example_fence.replace("main_task = asyncio.run(main())", "")
    ns: dict[str, Any] = {"__name__": "doc_ingest_truth_example"}
    exec(compile(code, FENCE_PATH, "exec"), ns)
    return ns


async def _drive_to_terminal_report(
    example_ns: dict[str, Any],
    compiled: Any,
    pool: asyncpg.Pool,
    schema: str,
    *,
    input_doc_ids: list[str],
) -> dict[str, Any]:
    """One fence run to its terminal PublishReport: create → drive to the
    review hold → resolve (approve) → drive terminal → the publish node's
    decoded result. The COMPILED graph is a parameter: ``app.get()``
    re-runs the build function on every call (re-baking the wiring's
    data args), so a test that mutates the fence's corpus must control
    WHEN the compile happens."""
    from taskq.workflows import FlowRunner
    from taskq.workflows.api._hitl import HitlClient

    runner = FlowRunner(compiled, pool, schema)
    batch = example_ns["IngestBatch"]
    run_id = (await runner.create_flow(input=batch(doc_ids=input_doc_ids))).flow_id
    assert await runner.drive(run_id, until="held") == "held", "the run never held"
    client = HitlClient(pool, schema=schema)
    (hold,) = await client.list(run_id)
    result = await client.resolve(hold.hold_id, {"verdict": "approve", "note": ""})
    assert result.status == "delivered"
    assert await runner.drive(run_id) == "terminal"
    raw = await pool.fetchval(
        f'SELECT result FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'publish'",
        run_id,
    )
    assert raw is not None, "the publish node left no result row"
    report = json.loads(raw) if isinstance(raw, str) else raw
    return dict(report["value"])


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-4(b) second face]; the marker is removed per the designed flip (the confirmation receipt).
def test_the_fence_drives_to_the_hold_not_past_it(example_fence: str) -> None:
    """The fence must drive ``until="held"`` before it lists and resolves
    the hold — the documented flow (hold → answer it) instead of the
    shipped ~166 s blind spin whose discarded 'max_ticks' return looks
    exactly like progress."""
    assert 'until="held"' in example_fence, (
        'the fence never drives until="held" — run_nightly_refresh\'s bare '
        "`await runner.drive(run_id)` spins to max_ticks PAST the review "
        "hold (~166s measured) and the verdict is discarded"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-4(c)]; the marker is removed per the designed flip (the confirmation receipt).
async def test_the_run_input_is_honored(
    module_pg_schema: Any,
    wf_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    example_ns: dict[str, Any],
) -> None:
    """The run's input is the run row's data (cut #7): a one-doc
    ``IngestBatch`` must fan a ONE-DOC map. Today the hardcoded corpus
    runs and the input is silently dropped."""
    from taskq.workflows import FlowRunner

    schema = module_pg_schema.schema_name
    compiled = example_ns["app"].get("doc_ingest_truth")
    runner = FlowRunner(compiled, wf_pool, schema)
    run_id = (
        await runner.create_flow(input=example_ns["IngestBatch"](doc_ids=["doc-only"]))
    ).flow_id
    assert await runner.drive(run_id, until="held") == "held"
    raw = await wf_conn.fetchval(
        f'SELECT result FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'ingest'",
        run_id,
    )
    assert raw is not None, "the ingest node left no result row"
    result = json.loads(raw) if isinstance(raw, str) else raw
    assert result["value"] == ["doc-only"], (
        f"the input was IGNORED — the ingest fanned {result['value']!r}, not the "
        "run's own input ['doc-only'] (the wiring's baked corpus wins; "
        "create_flow(input=…) is decorative)"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-4(d)]; the marker is removed per the designed flip (the confirmation receipt).
async def test_the_terminal_report_names_the_failures(
    module_pg_schema: Any,
    wf_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    example_ns: dict[str, Any],
) -> None:
    """Shape 8's claim — "the failures ride the report, named": with one
    document missing from the source, the router dead-letters it (the
    fence's own ``print`` proves the arm fires) and the terminal
    ``PublishReport`` must NAME it — in ``dead_lettered`` or ``failed`` —
    and must never list it as published."""
    source = example_ns["_DOC_SOURCE"]
    # Compile BEFORE the pop: get() re-runs the build function, so the
    # wiring bakes the FULL corpus into the ingest params here; the pop
    # then makes doc-011 UNREADABLE at body-run time (enrich_item's
    # _DOC_SOURCE.get miss → the Unreadable arm) while the baked list —
    # and the input — still fan it.
    compiled = example_ns["app"].get("doc_ingest_truth")
    doc_ids = [*sorted(source)]
    assert "doc-011" in doc_ids, "the fence's corpus changed — re-seat this pin"
    doomed_text = source.pop("doc-011")
    try:
        report = await _drive_to_terminal_report(
            example_ns, compiled, wf_pool, module_pg_schema.schema_name, input_doc_ids=doc_ids
        )
    finally:
        source["doc-011"] = doomed_text
    named = set(report["dead_lettered"]) | set(report["failed"])
    assert "doc-011" in named, (
        f"doc-011 dead-lettered and the terminal report is SILENT: "
        f"dead_lettered={report['dead_lettered']!r} failed={report['failed']!r} — "
        "the doc's 'the failures ride the report, named' is a lie"
    )
    assert "doc-011" not in set(report["published"]), (
        "a DEAD-LETTERED doc rode the published list — the report claims "
        "the run published the very document the router dead-lettered"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-4(e)]; the marker is removed per the designed flip (the confirmation receipt).
async def test_the_published_list_names_each_doc_once(
    module_pg_schema: Any,
    wf_pool: asyncpg.Pool,
    example_ns: dict[str, Any],
) -> None:
    """The collect's claim — the fan-in is ONE collect: the terminal
    report's ``published`` names each doc EXACTLY ONCE (no gather
    concatenation leaks into the typed verdict)."""
    report = await _drive_to_terminal_report(
        example_ns,
        example_ns["app"].get("doc_ingest_truth"),
        wf_pool,
        module_pg_schema.schema_name,
        input_doc_ids=sorted(example_ns["_DOC_SOURCE"]),
    )
    published = report["published"]
    assert len(published) == len(set(published)), (
        f"the published list double-counts: {len(published)} entries for "
        f"{len(set(published))} docs — the barrier's gather concatenated "
        "[summaries + entities] into the typed terminal verdict"
    )
