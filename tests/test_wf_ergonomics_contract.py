"""T17 — THE ERGONOMICS-CONTRACT PINS (the authoring session's paper cuts).

The BLOCKER/CRITICAL cures landed with T09's flow API: these probes were
captured RED against the pre-API tree (strict-xfail — the cut IS the red;
the red evidence: ``.measurements/t17-contract-first-run.txt``) and are
GREEN here — the marker's removal in THIS commit is the flip. The bar-walk
compiles the authoring agent's original graph (unchanged shapes) with
ZERO boilerplate: the bar is "first-try correct, no boilerplate, IDE
autocompletion resolves the wiring."

The disposition ledger — cut → severity → disposition → where it landed →
the re-test — lives in ``.measurements/t17-dispositions.md``.
"""

from __future__ import annotations

import inspect
import subprocess  # Why: the abstraction check IS a grep over the tree; fixed argv, no user input.
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from taskq.workflows import (
    WorkflowApp,
    build,
    gather,
    sink,
    step,
)
from taskq.workflows.api._runner import FlowRunner

# ── the ABSTRACTION CONTRACT's banned vocabulary (the campaign domain) ──
#: The repo-bound surfaces ship in the abstract ``doc_ingest`` domain; the
#: authoring session's own campaign vocabulary must never leak into them.
#: The same repo-wide check tickets 13/14/16 cite (T17's scope).
_BANNED_DOMAIN_TERMS: tuple[str, ...] = (
    "capex",
    "cbre",
    "portfolio",
    "sustainability",
    "ensemble",
    "hierarchy_research",
    "await_user_data",
)


def test_shipped_surfaces_carry_no_campaign_vocabulary() -> None:
    """THE ABSTRACTION CONTRACT (T17): grep the shipped surfaces for the
    campaign domain's terms — zero hits, the examples/docs/code live in
    the abstract doc_ingest domain."""
    repo = Path(__file__).resolve().parents[1]
    # The surfaces PR-5's commits ship (the ticket's list, scoped to the
    # workflow round's files): the estate's pre-existing tests carry
    # generic English names that collide with the banned list
    # (``test_di_solver.py``'s ``_PortfolioClient`` fixture predates the
    # program); the contract binds what THIS round lands, not the tree's
    # history.
    surfaces = [
        "src/taskq/workflows",
        "docs/guides/workflows.md",
        "docs/api-reference/workflows.md",
        "examples",
        "tests/typeprobe",
    ]
    surfaces += [str(p.name) for p in (repo / "tests").glob("test_wf_*.py")]
    result = subprocess.run(
        [
            "grep",
            "-riE",
            "|".join(_BANNED_DOMAIN_TERMS),
            *[str(repo / s) for s in surfaces],
            # This check names the banned terms in its own tuple — excluded
            # by name, or the check convicts itself.
            "--exclude=test_wf_ergonomics_contract.py",
            "--exclude-dir=__pycache__",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.stdout == "", (
        "the abstraction contract is violated — campaign-domain vocabulary "
        f"reached a shipped surface:\n{result.stdout[:2000]}"
    )


# ── the bar-walk's module-level wiring (the annotations resolve) ────────


class DocIngest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Vec(BaseModel):
    v: list[float]


async def _fetch(ctx: Any, params: DocIngest) -> Report:
    return Report(ref=f"report-{params.doc_id}")


async def _embed(ctx: Any, report: Report) -> Vec:
    return Vec(v=[1.0])


async def _summarize(ctx: Any, reports: list[Report]) -> dict[str, int]:
    return {"count": len(reports)}


async def _reduce_join(ctx: Any, a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
    return {"total": a["n"] + b["n"]}


async def _stage_a(ctx: Any, params: DocIngest) -> dict[str, int]:
    return {"n": 1}


async def _stage_b(ctx: Any, params: DocIngest) -> dict[str, int]:
    return {"n": 2}


async def _tail(ctx: Any, total: dict[str, int]) -> dict[str, int]:
    return total


async def _chooser(ctx: Any, params: DocIngest) -> dict[str, str]:
    return {"pick": "b"}


async def _branch_a(ctx: Any, params: DocIngest) -> dict[str, int]:
    return {}


async def _branch_b(ctx: Any, params: DocIngest) -> dict[str, int]:
    return {}


def test_bar_walk() -> None:
    """THE BAR WALK (T17's red-first 2): the authoring agent's original
    graph — the shapes the paper cuts were hit on — compiles with zero
    boilerplate: plain-type data, Promise wiring, a gather-join,
    sequencing, a consumed terminal, the input parameter on create_flow."""
    app = WorkflowApp()

    @app.workflow("doc_ingest")
    def doc_ingest() -> object:
        fetched = step(_fetch, DocIngest(doc_id="d1"))  # Promise[Report]
        embed_p = step(_embed, fetched)  # Promise[Vec] — sequenced
        sink(embed_p)  # the explicit fire-and-forget (recorded, never silent)
        both = gather([fetched, fetched])  # the ALL-upstream join
        return build(step(_summarize, both, key="summarize"))

    compiled = app.get("doc_ingest")
    assert compiled is not None
    keys = set(compiled.node_keys())
    assert {"_fetch", "_embed", "summarize"} <= keys
    compiled.validate()  # the zero-false-positive bar: the ORIGINAL graph is clean


def test_paper_cut_1_join_user_body() -> None:
    """Cut #1 (BLOCKER): a join's USER reducer body, spelled in the wiring —
    no out-of-engine decode, no multi-flow glue; its result cascades."""
    app = WorkflowApp()

    @app.workflow("join_body_walk")
    def join_body() -> object:
        a = step(_stage_a, DocIngest(doc_id="d1"), key="stage_a")
        b = step(_stage_b, DocIngest(doc_id="d1"), key="stage_b")
        reducer = step(_reduce_join, a, b, key="reducer")
        return build(step(_tail, reducer, key="tail"))

    compiled = app.get("join_body_walk")
    assert compiled is not None
    # the reducer node fans in BOTH stages; `tail` consumes the reducer's
    # promise — the cascade spelled in the wiring.
    assert compiled.parents_of("reducer") == ["stage_a", "stage_b"]
    assert compiled.parents_of("tail") == ["reducer"]
    compiled.validate()


def test_paper_cut_7_create_flow_takes_input() -> None:
    """Cut #7 (friction): ``create_flow(spec, input)`` — cross-flow data
    rides the run row, never a Python closure."""
    app = WorkflowApp()

    @app.workflow("input_walk")
    def input_walk() -> object:
        return build(step(_tail, step(_stage_a, DocIngest(doc_id="d"), key="a"), key="tail"))

    compiled = app.get("input_walk")
    assert compiled is not None
    # the signature exists and accepts the input (the runtime behavior is
    # T09's runner pins; this walk pins the ERGONOMIC SHAPE: the parameter
    # is there, IDE-discoverable).
    assert "input" in inspect.signature(FlowRunner.create_flow).parameters


def test_paper_cut_4_dispatch_time_predicate() -> None:
    """Cut #4 (CRITICAL): a Maybe guard decided at DISPATCH, not create —
    a sibling-reading guard is expressible."""
    app = WorkflowApp()

    @app.workflow("dispatch_guard_walk")
    def dispatch_guard() -> object:
        pick = step(_chooser, DocIngest(doc_id="d"), key="pick")
        guarded = step(
            _branch_a,
            pick,
            key="branch_a",
            skip=lambda state: (state["results"].get("pick") or {}).get("pick")  # pyright: ignore[reportUnknownArgumentType, reportAttributeAccessIssue]  # Why: the predicate receives the runner's state dict — the walk's shape is the runner's contract.
            == "b",
        )
        sink(guarded)  # the skipped branch's drop is EXPLICIT (recorded)
        return build(step(_branch_b, pick, key="branch_b"))

    compiled = app.get("dispatch_guard_walk")
    assert compiled is not None
    # the guard is stored as a CALLABLE on the node's definition — decided
    # when the node dispatches, against the flow's state.
    assert callable(compiled.skip_predicate("branch_a"))
    compiled.validate()


def test_promise_type_story() -> None:
    """The wiring's typed vocabulary is REAL: a promise carries its
    producer's key + declared data type (the compile's compatibility rule
    reads it); promises are recorder-minted, never hand-built."""
    app = WorkflowApp()

    @app.workflow("promise_types")
    def promise_types() -> object:
        fetched = step(_fetch, DocIngest(doc_id="d"), key="fetch")
        assert fetched.key == "fetch"
        # The DECLARATION is the annotation's spelling; the RESOLVED type
        # is the validator's (the zero-false-positive compat rule reads
        # the resolved hints — under future-annotations the declaration
        # arrives a string, and BOTH faces are the contract).
        from taskq.workflows.api._hints import body_hints

        assert body_hints(_fetch)["return"] is Report
        assert fetched.data_type == "Report"
        return build(fetched)

    app.get("promise_types")
