# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""``ctx.substep`` in a chain source, across a retry and emit parks (#743).

The emit park sets the source row's physical ``attempt`` to 0, so the next
claim runs at physical 1 again while ``claim_epoch`` keeps rising. The
substep claims at the PHYSICAL attempt, so a ``failed`` row written at
physical 2 before a park sits at a HIGHER attempt than the ``succeeded`` row a
later claim writes at physical 1. The memo must answer with the later claim's
row (``claim_epoch`` first); ranked by attempt first, the old failure wins and
every resume re-runs the side effect.
"""

from __future__ import annotations

import contextlib
import enum
from typing import Any

import asyncpg

from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, chain_source
from taskq.workflows.chain import DONE, Chain, Route, Step, chain_start


class _Out(enum.Enum):
    OK = "ok"


async def _leaf(ctx: object, item: dict[str, object]) -> _Out:
    return _Out.OK


_CHAIN = Chain(
    name="substep-src-chain",
    start="leaf",
    steps={"leaf": Step(body=_leaf, outcomes=_Out, route=Route({_Out.OK: DONE}))},
)
_PAGES = [[0, 1], [2, 3], [4, 5], [6, 7]]


async def test_a_chain_source_substep_replays_its_success_after_a_retry_then_emit_parks(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """Logical attempt 1 fails the body. Attempt 2 (physical 2): substep x
    fails once (caught), the source emits and parks (physical -> 0). The
    resumed claim (physical 1) re-runs x, which succeeds. Every later
    park/resume must REPLAY that success: x ran ``["fail", "ok"]``, once each."""
    x_calls: list[str] = []
    entries: list[int] = []

    async def x_fn() -> str:
        x_calls.append("fail" if not x_calls else "ok")
        if len(x_calls) == 1:
            raise ValueError("x fails once")
        return "ok"

    async def src(ctx: Any) -> None:
        entries.append(ctx.attempt)
        if len(entries) == 1:
            raise RuntimeError("the first logical attempt fails the body")
        with contextlib.suppress(ValueError):
            await ctx.substep("x", x_fn)
        cursor = await ctx.cursor()
        for p in range(int(cursor.get("page", -1)) + 1, len(_PAGES)):
            await ctx.emit_batch(
                [
                    chain_start(_CHAIN, {"i": i}, map_index=i, trace_id=f"sub-{i}")
                    for i in _PAGES[p]
                ],
                cursor={"page": p},
            )

    app = WorkflowApp()

    @app.workflow("substep_chain_source_retry_then_park", max_in_flight=2)
    def wf() -> Promise[object]:
        return build(chain_source(_CHAIN, src, key="src", max_attempts=3))

    runner = FlowRunner(app.get("substep_chain_source_retry_then_park"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=2000) == "terminal"
    x_rows = [
        dict(r)
        for r in await wf_conn.fetch(
            f'SELECT attempt, claim_epoch, status FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key LIKE '%/x' ORDER BY created_at, id",
            flow_id,
        )
    ]
    # the shape that exercises the order: >= 2 resumes after the retry, and
    # the failure sits at a higher physical attempt than the later success.
    assert len(entries) >= 4, f"the source did not park twice after its retry: {entries}"
    assert [r["status"] for r in x_rows[:2]] == ["failed", "succeeded"], x_rows
    assert x_rows[0]["attempt"] > x_rows[1]["attempt"], x_rows
    assert x_rows[0]["claim_epoch"] < x_rows[1]["claim_epoch"], x_rows
    assert x_calls == ["fail", "ok"], (
        f"a resume re-ran x's recorded success: {x_calls}; rows {x_rows}"
    )
