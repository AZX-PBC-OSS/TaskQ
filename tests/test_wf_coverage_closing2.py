# Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""THE COVERAGE-CLOSING PINS, part 2 (the estate floor): the capture
module's pure functions (the truncation loop, the redact chain, the
policy refusal), the reducer resolution's definition fallback, the
engine's NodeSpec guards, the ctx.step's encode branches. Pure
functions first (fast, exact), the live paths second."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from taskq.workflows import Promise


class Ingest(BaseModel):
    doc_id: str


# ── _capture.py: the pure capture contract ──────────────────────────────


def test_build_capture_refuses_an_unknown_policy() -> None:
    from taskq.workflows._capture import build_capture

    with pytest.raises(ValueError, match="capture policy must be one of"):
        build_capture(policy="sometimes", node_input=None, error=None)


def test_build_capture_none_is_the_refusal() -> None:
    from taskq.workflows._capture import build_capture

    assert build_capture(policy="none", node_input="x", error="boom") is None


def test_build_capture_truncates_the_multibyte_payload_under_the_cap() -> None:
    """The halving loop: a multi-byte-heavy payload (each char 3 UTF-8
    bytes) truncates to the byte cap — the cap is BYTES, never chars.
    (The multibyte text is the input's VALUE — the redact chain's
    processing is the caller's; the capture passes it through.)"""
    from taskq.workflows._capture import build_capture, utf8_byte_len

    heavy = "冰" * 4000  # 12 000 bytes of payload
    capture = build_capture(
        policy="errors-only",
        node_input=heavy,
        error="e",
        max_bytes=1024,
        redact=lambda text: text,  # the identity hook: the raw text walks
    )
    assert capture is not None
    text = str(capture.get("input"))
    assert utf8_byte_len(text) <= 1024, utf8_byte_len(text)


def test_build_capture_redact_chain_runs_before_truncation() -> None:
    """The redact HOOK: the marker is masked before the truncation —
    the persisted row carries no marker (pin 11: masks are irreversible,
    so truncating scrubbed text cannot reveal what the mask hid)."""
    from taskq.workflows._capture import build_capture

    capture = build_capture(
        policy="errors-only",
        node_input="SECRET_MARKER the rest of the payload text",
        error=None,
        redact=lambda text: text.replace("SECRET_MARKER", "[MASKED]"),
        max_bytes=32,
    )
    assert capture is not None
    text = str(capture)
    assert "SECRET_MARKER" not in text, "the marker rode the capture"


def test_build_capture_no_fields_is_none() -> None:
    from taskq.workflows._capture import build_capture

    assert build_capture(policy="errors-only", node_input=None, error=None) is None


# ── engine.py: the NodeSpec guards + the reducer body ───────────────────


async def test_insert_node_the_deps_guard(wf_conn: Any, wf_schema: str) -> None:
    """The joined node's counter guard: the deps_pending is the declared
    parent count's twin — a mismatching spec is the named refusal."""
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows._types import NodeSpec
    from taskq.workflows.engine import insert_node

    wsql = WorkflowSql.build(wf_schema)
    with pytest.raises(ValueError, match="deps_pending must be >= 0"):
        await insert_node(
            wf_conn,
            wsql,
            NodeSpec(
                flow_id="018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f",
                step_key="n",
                actor="wf",
                queue="q",
                payload={},
                parents=("p",),
                deps_pending=-1,
            ),
        )
    with pytest.raises(ValueError, match="must equal the declared parent count"):
        await insert_node(
            wf_conn,
            wsql,
            NodeSpec(
                flow_id="018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f",
                step_key="n",
                actor="wf",
                queue="q",
                payload={},
                parents=("p", "p2"),
                deps_pending=1,
            ),
        )


def test_join_metadata_the_child_driven_shape_record() -> None:
    """T07's shape record: the child-driven escape rides the ROW (the
    rederive reads it; the record never guesses)."""
    from taskq.backend._protocol import JobId
    from taskq.workflows._types import ConsumerBinding, _join_metadata

    meta = _join_metadata(
        JobId("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"),
        (ConsumerBinding(step_key="c", actor="wf", queue="q"),),
        child_driven=True,
    )
    assert meta["join_shape"] == "child_driven"
    assert meta["blocking_reason"] == "join"


# ── context.py: the encode branches ─────────────────────────────────────


def test_encode_step_result_the_model_and_the_plain_form() -> None:
    from taskq.workflows.context import WorkflowSteps

    class Out(BaseModel):
        n: int

    encoded_model = WorkflowSteps._encode_step_result(Out(n=1))
    assert '"n"' in encoded_model
    encoded_plain = WorkflowSteps._encode_step_result({"n": 1})
    assert '"n"' in encoded_plain


# ── _reducers.py: the resolution's verdict (the adapter's cure) ─────────


async def test_resolve_reducer_the_definition_fallback(wf_pool: Any, wf_schema: str) -> None:
    """THE ADAPTER'S DEFECT, CURED AT THE SOURCE (the wedged-hold cure,
    2026-10-09): the old adapter called the registry's body with ONE
    argument (``definition_body(None)``) — a step body (ctx + params)
    raised TypeError, the fire's tx rolled back, and the sweep re-fired
    FOREVER (the wedged arm, the drive's max_ticks). THE CURE'S VERDICT:
    the registry's bodies are STEP bodies — never reducers — so the
    resolution returns NO body for a graph that resolves (the row's own
    claim owns the execution, the wired args), and the LOUD face is
    reserved for the name that resolves NOWHERE (the R2-2 deployment
    defect). The RED evidence (the pin's own recorded history): the
    adapter's observed TypeError."""

    del wf_pool, wf_schema  # the registry is process-level; no DB here
    from taskq.workflows import WorkflowApp, build, step

    ran: dict[str, bool] = {}

    async def join_body(ctx: Any, items: list[str]) -> list[str]:
        ran["join"] = True
        return items

    app = WorkflowApp()

    @app.workflow("reducer_fallback_xf")
    def reducer_fallback() -> Promise[object]:
        return build(step(join_body, Ingest(doc_id="d"), key="solo"))

    app.get("reducer_fallback_xf")  # the compile REGISTERS the bodies (D1)

    from taskq.workflows._reducers import resolve_flow_reducer

    resolved = resolve_flow_reducer(
        "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f", "solo", workflow_name="reducer_fallback_xf"
    )
    # THE CURE'S VERDICT: the registered STEP body is NEVER adapted to
    # the reducer convention — no body (the claim's own execution), and
    # the graph RESOLVED (the name is registered here) so the face is
    # the healthy one, never the loud stamp.
    assert resolved.body is None, (
        "the resolution adapted a STEP body to the reducer convention — "
        "the TypeError machine is back"
    )
    assert resolved.loud is not True, (
        "a graph that RESOLVES in this process stamped the loud "
        "deployment-defect face — the healthy shape's record would lie"
    )
    assert ran.get("join") is not True, (
        "the resolution RAN the step body itself — the claim owns the execution; RE-DERIVE THE PIN"
    )
