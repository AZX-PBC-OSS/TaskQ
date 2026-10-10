"""THE PROGRESS READ'S IDENTITY PINS (the consumer-face lane's CURE 1 —
the verified "not useful" conviction): the display's every node entry
carried the ROW UUID as the only identity — ``status``/``pct``/
``message``/``error_class`` with NO ``step_key``, NO ``map_index`` — so
the cowork-UI holding a display could not say WHICH node any entry
belonged to without an out-of-band join against the ledger.

THE CURE'S SHAPE (the read face carries the node's identity):

* every entry carries ``step_key`` + ``map_index`` — the ROW'S OWN
  fields, read off the LEDGER (the identity's authority); an entry the
  ledger has not seen yet (a state-channel ghost) carries ``None``
  until the ledger lands (the LEDGER-WINS pass stamps it);
* the KEYED VIEW (:func:`taskq.workflows._progress_read.keyed_display`)
  — the step_key'd dict, ``{"ocr_node": {"pct": 45, ...}}`` — the
  consumer's render WITHOUT the out-of-band join. A map's children
  share one step_key (``<src>.item``), so a child's key carries its
  ``map_index`` (``"fetch.item[3]"``) — keying bare would silently
  collapse N children into one entry (last-wins, the lie).

Red-first: the pins ran RED at the pre-cure head (the identity keys
absent → the assert fires; the keyed view absent → ImportError); the
reds are captured in ``.measurements/cons-cure1-reds.json``.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    step,
)
from taskq.workflows._progress_read import (
    display_key,
    keyed_display,
    rebuild_display,
    run_display,
)
from taskq.workflows._sql import WorkflowSql
from tests._wf_fixtures import RedLog

pytestmark = pytest.mark.integration


@pytest.fixture
def cons_redlog() -> RedLog:
    """The red sink for the consumer-face lane's CURE 1 pins."""
    log = RedLog("cons-cure1-reds.json")
    return log


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


def _loads(raw: Any) -> Any:
    return json.loads(raw) if isinstance(raw, str) else raw


async def _map_flow(
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    children: int,
    name: str,
) -> tuple[JobId, FlowRunner]:
    """A MAP flow: the source returns N indices; the item body emits one
    honest progress emission and returns {"risk": i}; a sink consumes
    the join."""
    app = WorkflowApp()

    async def fetch(ctx: StepContext, params: Ingest) -> list[int]:
        return list(range(children))

    async def item(ctx: StepContext, value: int) -> dict[str, object]:
        await ctx.progress(((value + 1) * 100) // children, f"child {value}", {"child": value})
        return {"risk": value}

    async def collect(ctx: StepContext, items: list[dict[str, object]]) -> Report:
        return Report(ref=f"{len(items)}")

    @app.workflow(name)
    def map_flow() -> Promise[object]:
        source = step(fetch, Ingest(doc_id="d1"), key="fetch")
        items = map_source(source, item)
        collected = step(collect, items)
        return build(collected)

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    return flow_id, runner


# ── THE IDENTITY ON EVERY ENTRY ──────────────────────────────────────────


async def test_every_display_entry_carries_the_rows_own_identity(
    cons_redlog: RedLog, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """THE IDENTITY: after the source forks, ``run_display``'s every
    entry carries ``step_key`` + ``map_index`` — the LEDGER's own fields:
    the source step keyed ``fetch`` (``map_index`` None — not a map
    child), the children keyed ``fetch.item`` with their OWN
    ``map_index``. The display a consumer renders is the node's identity
    + its progress, never a uuid it cannot name."""
    flow_id, runner = await _map_flow(wf_schema, wf_pool, children=3, name="identity_map_flow")
    await runner.tick(flow_id)  # the source runs + forks; children pending
    disp = await run_display(wf_pool, wf_sql, flow_id)
    assert disp, "the display reads the run's nodes"

    entries = list(disp.values())
    missing = [e for e in entries if "step_key" not in e or "map_index" not in e]
    if missing:
        cons_redlog.red(
            "cons1-identity-on-every-entry",
            "run_display's entries carry no step_key/map_index (the uuid is "
            "the only identity — the cowork-UI cannot say which node)",
            {"entries": missing},
        )
    assert not missing, (
        f"{len(missing)} of {len(entries)} display entries carry no "
        "step_key/map_index — the display's identity is the row UUID "
        "alone, unusable without the out-of-band join"
    )

    # The identity is the ROW'S OWN: the source's fields and the
    # children's, read off the ledger.
    by_key = {e.get("step_key"): e for e in entries if e.get("map_index") is None}
    assert by_key.get("fetch") is not None, "the source step's entry is keyed 'fetch'"
    children = {
        (e.get("step_key"), e.get("map_index")) for e in entries if e.get("map_index") is not None
    }
    assert children == {("fetch.item", 0), ("fetch.item", 1), ("fetch.item", 2)}, (
        "the map children carry their OWN step_key + map_index"
    )


# ── THE KEYED VIEW ───────────────────────────────────────────────────────


async def test_the_keyed_view_is_the_consumers_render_without_the_join(
    wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """THE KEYED VIEW: ``keyed_display(run_display(...))`` is the
    step_key'd dict — ``{"fetch": {...}, "fetch.item[0]": {...}}`` — the
    consumer's render keyed by the node's NAME, no out-of-band ledger
    join. Every entry is the SAME dict the uuid view holds (one display,
    two keyings — never a second model)."""
    flow_id, runner = await _map_flow(wf_schema, wf_pool, children=2, name="keyed_map_flow")
    await runner.tick(flow_id)
    # The children emit: drive one child's claim+finalize so pct renders
    # (the keyed entry's shape is the conviction's "ocr_node: {pct: …}").
    await runner.drive(flow_id, until="terminal")

    disp = await run_display(wf_pool, wf_sql, flow_id)
    keyed = keyed_display(disp)

    assert "fetch" in keyed, "the source step renders under its own step_key"
    fetch_entry = keyed["fetch"]
    assert {"status", "pct", "message", "error_class", "step_key", "map_index"} <= set(fetch_entry)
    assert fetch_entry["status"] == "succeeded"

    # The map children: the map_index DISAMBIGUATES (a bare step_key
    # would collapse the siblings last-wins).
    assert "fetch.item[0]" in keyed and "fetch.item[1]" in keyed
    assert "fetch.item" not in keyed, "a bare child key would collapse the siblings — never keyed"
    child0 = keyed["fetch.item[0]"]
    assert child0["pct"] == 50 and child0["message"] == "child 0"

    # ONE display, two keyings: the keyed entries ARE the uuid entries.
    uuid_view = {id(v): v for v in disp.values()}
    assert all(id(v) in uuid_view for v in keyed.values())


async def test_keyed_view_omits_the_unnamed_and_keys_children_by_map_index() -> None:
    """THE PURE FUNCTION'S OWN LAW: an entry with no step_key (a
    state-channel ghost the ledger has not landed) cannot be NAMED — it
    is omitted from the keyed view, still present in the uuid view; a
    named child keys ``step[map_index]``."""
    display = {
        "uuid-ghost": {
            "status": None,
            "pct": 45,
            "message": None,
            "error_class": None,
            "step_key": None,
            "map_index": None,
        },
        "uuid-step": {
            "status": "running",
            "pct": 45,
            "message": "ocr",
            "error_class": None,
            "step_key": "ocr_node",
            "map_index": None,
        },
        "uuid-child": {
            "status": "running",
            "pct": 10,
            "message": None,
            "error_class": None,
            "step_key": "ocr_node.item",
            "map_index": 3,
        },
    }
    keyed = keyed_display(display)
    assert set(keyed) == {"ocr_node", "ocr_node.item[3]"}
    assert keyed["ocr_node"]["pct"] == 45
    # The ghost is untouched, just unnamed (its keys unchanged).
    assert display["uuid-ghost"]["step_key"] is None
    assert "uuid-ghost" not in keyed and len(keyed) == 2

    assert display_key("ocr_node", None) == "ocr_node"
    assert display_key("ocr_node.item", 3) == "ocr_node.item[3]"
    assert display_key(None, None) is None
    assert display_key("", 0) is None


async def test_the_ledger_wins_pass_stamps_the_identity_on_ghost_entries() -> None:
    """THE LEDGER WINS (the fence's teeth, extended to the identity): a
    ghost entry born from the state channel — the ledger row MISSED by
    the first pass — gets its status AND its step_key/map_index stamped
    when the ledger row exists; a display can never render a named lie."""
    ledger = [
        {
            "id": "n1",
            "step_key": "fetch",
            "map_index": None,
            "status": "failed",
            "error_class": "ValueError",
        }
    ]
    events: list[dict[str, Any]] = []
    state_rows: list[dict[str, Any]] = []
    disp = rebuild_display(ledger, state_rows, events)
    assert disp["n1"]["step_key"] == "fetch"
    assert disp["n1"]["map_index"] is None
    assert disp["n1"]["status"] == "failed"
    assert _loads(json.dumps(disp["n1"]))["step_key"] == "fetch"
