"""The workflow-run explorer's row reads (T11 + the run-explorer
addendum): the run's graph, states, holds, and timeline assembled FROM
THE ROWS ALONE — the rows-alone law; no surface owns state the engine
doesn't. This module owns the ONE grouped read both surfaces (the
pages, the machine routes) share.

Two pure derivations live here:
* :func:`rows_mermaid` — the run graph's Mermaid text from ``jobs`` +
  ``wf_edge`` (the compile's own vocabulary; the run's LIVE graph is a
  row fact, not a definition import — the admin never imports the
  workflow's module);
* :func:`run_revision`-equivalent — the SSE feed's monotonic revision
  (the ledger's max ids; the rows are the durable ring the replay
  reads).

The status derivation is THE §17.5 derivation (``workflows._status``) —
one engine, three surfaces (backend, SSE, CLI), no mapping drift.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final

from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike

if TYPE_CHECKING:
    # THE §16.1 IMPORT LAW's typing-only carve-out: the name exists for
    # the annotations alone — it is NEVER a runtime module-scope import
    # (the fresh-interpreter probes prove the package stays unloaded).
    from taskq.workflows._status import NodeView

__all__ = [
    "NOT_INSTALLED",
    "RunNode",
    "RunView",
    "fetch_run_view",
    "rows_mermaid",
    "run_state_json",
    "status_class",
]

#: The status → the CSS class the page's legend declares (the state
#: colors' single source is admin/_constants's chip map; the graph's
#: fills key off THESE names).
STATUS_CLASS: Final[dict[str, str]] = {
    "pending": "wf-st-pending",
    "scheduled": "wf-st-scheduled",
    "running": "wf-st-running",
    "succeeded": "wf-st-succeeded",
    "failed": "wf-st-failed",
    "cancelled": "wf-st-cancelled",
    "crashed": "wf-st-crashed",
    "abandoned": "wf-st-abandoned",
    "skipped": "wf-st-skipped",
}

#: The workflow tables' absence (a schema migrated before the workflow
#: series) is the pages' degrade, not a 500.
NOT_INSTALLED = "workflows not installed; run taskq migrate up to enable"

_RUN_ROOT_SQL = (
    "SELECT id, actor, status, created_at, finished_at, cancel_requested_at, "
    "payload, error_class, error_message, metadata->>'workflow' AS workflow "
    "FROM \"{schema}\".jobs WHERE id = $1 AND step_key = '__flow__'"
)

_RUN_EDGES_SQL = 'SELECT parent_id, child_id FROM "{schema}".wf_edge WHERE child_id = ANY($1)'

_RUN_HOLDS_SQL = (
    "SELECT id, node_key, signal_name, hold_epoch, payload, payload_schema, "
    'status, created_at, expires_at, resolved_at FROM "{schema}".wf_signals '
    "WHERE workflow_id = $1 ORDER BY id"
)


@dataclass(slots=True)
class RunNode:
    """One node row's view (the graph + the panel's shared shape)."""

    key: str
    status: str
    deps_pending: int = 0
    blocking_reason: str | None = None
    absorbed: bool = False
    error_class: str | None = None
    error_message: str | None = None
    map_children: int = 0
    map_done: int = 0
    hold: dict[str, Any] | None = None
    parents: tuple[str, ...] = ()

    def view(self) -> NodeView:
        """The §17.5 derivation's input (the one conversion — the same
        law the CLI's analysis module states)."""
        # THE §16.1 IMPORT LAW: the workflows import stays LAZY (the
        # module-scope import coupled every admin-page load to the
        # package — this module is the one surface the law's pin had
        # caught importing at module scope).
        from taskq.workflows._status import NodeView

        return NodeView(
            status=self.status,
            deps_pending=self.deps_pending,
            blocking_reason=self.blocking_reason,
            held=self.hold is not None,
            absorbed=self.absorbed,
        )


@dataclass(slots=True)
class RunView:
    """One run's assembled rows (the page's and the SSE snapshot's
    shared source — one read, two renderers)."""

    run_id: str
    workflow: str
    root_status: str
    created_at: str | None
    finished_at: str | None
    input_payload: Any
    cancel_requested_at: str | None
    error_class: str | None
    error_message: str | None
    nodes: list[RunNode]
    holds: list[dict[str, Any]]
    derived: str = ""

    def derive(self) -> str:
        """THE derivation's output, cached onto the view (the report's
        status line and the G7 check read the same value)."""
        if not self.derived:
            from taskq.workflows._status import derive_workflow_status

            self.derived = derive_workflow_status(tuple(n.view() for n in self.nodes))
        return self.derived


def _normalize_hold(row: Mapping[str, Any]) -> dict[str, Any]:
    """The hold row's json-safe shape: the uuid7 id (the reply handle)
    and the clocks as text — the boot JSON and the SSE frames are JSON,
    and a UUID in them is the TypeError the frame must never carry."""
    out = dict(row)
    for key, value in out.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        elif isinstance(value, uuid.UUID):
            out[key] = str(value)
    return out


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


async def fetch_run_view(conn: ConnLike, schema: str, run_id: uuid.UUID) -> RunView | None:
    """The run's rows in one connection — the grouped read the page, the
    boot JSON, and every SSE frame share (one read, all renderers)."""
    # THE RENDERED BUNDLE (the render seam: the schema + the {terminal}
    # vocabulary + the doubled braces — a raw .replace("{schema}", …)
    # left the terminal token in the SQL and the statement died on the
    # stray brace).
    from taskq.workflows._sql import WorkflowSql

    wsql = WorkflowSql.build(schema)
    root = await conn.fetchrow(_RUN_ROOT_SQL.format(schema=schema), run_id)
    if root is None:
        return None
    node_rows = await conn.fetch(wsql.workflow_nodes, run_id)
    node_ids = [r["id"] for r in node_rows]
    edge_rows = await conn.fetch(_RUN_EDGES_SQL.format(schema=schema), node_ids) if node_ids else []
    hold_rows = await conn.fetch(_RUN_HOLDS_SQL.format(schema=schema), run_id)
    progress = {
        r["step_key"]: (int(r["done"]), int(r["total"]))
        for r in await conn.fetch(wsql.workflow_map_progress, root["id"])
    }
    return _run_view_from_rows(
        dict(root),
        [dict(r) for r in node_rows],
        [dict(r) for r in edge_rows],
        [dict(r) for r in hold_rows],
        progress,
    )


def _run_view_from_rows(
    root: Mapping[str, Any],
    node_rows: Sequence[Mapping[str, Any]],
    edge_rows: Sequence[Mapping[str, Any]],
    hold_rows: Sequence[Mapping[str, Any]],
    progress: dict[str, tuple[int, int]],
) -> RunView:
    """The assembly: the rows → the view (the edges' parent keys ride the
    node map; the held marker joins the signal rows)."""
    id_to_key = {r["id"]: r["step_key"] for r in node_rows}
    parents_of: dict[str, list[str]] = {}
    for e in edge_rows:
        parent_key = id_to_key.get(e["parent_id"])
        child_key = id_to_key.get(e["child_id"])
        if parent_key and child_key:
            parents_of.setdefault(child_key, []).append(parent_key)
    holds = [_normalize_hold(h) for h in hold_rows]
    held_by_node = {h["node_key"]: h for h in holds if h["status"] == "held"}
    nodes = [
        RunNode(
            key=r["step_key"],
            status=r["status"],
            deps_pending=r["deps_pending"],
            blocking_reason=r["blocking_reason"],
            absorbed=r["absorbed"],
            error_class=r["error_class"],
            error_message=r["error_message"],
            map_children=progress.get(r["step_key"], (0, 0))[1],
            map_done=progress.get(r["step_key"], (0, 0))[0],
            hold=dict(held_by_node[r["step_key"]]) if r["step_key"] in held_by_node else None,
            parents=tuple(sorted(parents_of.get(r["step_key"], []))),
        )
        for r in node_rows
    ]
    payload: object = root["payload"]
    if isinstance(payload, str):
        with contextlib.suppress(ValueError):
            payload = _json_loads(payload)
    return RunView(
        run_id=str(root["id"]),
        workflow=root["workflow"] or root["actor"],
        root_status=root["status"],
        created_at=_iso(root["created_at"]),
        finished_at=_iso(root["finished_at"]),
        input_payload=payload,
        cancel_requested_at=_iso(root["cancel_requested_at"]),
        error_class=root["error_class"],
        error_message=root["error_message"],
        nodes=nodes,
        holds=holds,
    )


def rows_mermaid(view: RunView) -> str:
    """The run graph's Mermaid text FROM THE ROWS — the compile's shape
    vocabulary, the run's live edge set. Byte-stable for identical rows
    (sorted emission, the compile's own law). A node the map collapsed
    renders as the hexagon-with-counter shape ({{...}}); ZERO child
    boxes — the child detail lives in the paginated panel."""
    lines: list[str] = ["flowchart TD"]
    for node in sorted(view.nodes, key=lambda n: n.key):
        collapsed = node.map_children > 0
        open_, close = ("{{", "}}") if collapsed else ("[", "]")
        label = node.key
        if node.hold is not None:
            label += " ⏳"
        if collapsed:
            label += f" {node.map_done}/{node.map_children}"
        lines.append(f'    {node.key}{open_}"{label}"{close}')
    edges: list[str] = []
    for node in sorted(view.nodes, key=lambda n: n.key):
        for parent in sorted(node.parents):
            edges.append(f"    {parent} --> {node.key}")
    lines.extend(sorted(edges))
    return "\n".join(lines) + "\n"


def run_state_json(view: RunView, *, seq: int) -> dict[str, Any]:
    """The SSE snapshot's payload: the DERIVED status + every node's
    state (the client patches by ``data-node-key``; a full replacement —
    the reconnect's replay IS this snapshot, so a killed connection
    never loses state)."""
    return {
        "seq": seq,
        "run_id": view.run_id,
        "status": view.derive(),
        "root_status": view.root_status,
        "nodes": [
            {
                "key": n.key,
                "status": n.status,
                "hold": n.hold is not None,
                "map_done": n.map_done,
                "map_children": n.map_children,
            }
            for n in sorted(view.nodes, key=lambda n: n.key)
        ],
        "holds": [
            {
                "hold_id": h["id"],
                "signal": h["signal_name"],
                "node": h["node_key"],
                "epoch": h["hold_epoch"],
            }
            for h in view.holds
        ],
    }


def status_class(status: str) -> str:
    """The status's CSS class (the graph fills + the badges key off the
    ONE map — the bare-<path> hexagon pins the path selector)."""
    return STATUS_CLASS.get(status, "wf-st-pending")


def summarize_map_children(
    progress_rows: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[int, int]]:
    """The per-map done/total pairs from ``WORKFLOW_MAP_PROGRESS_SQL``'s
    rows (the collapsed hexagon's counter — one read, no second
    instrument)."""
    return {r["step_key"]: (int(r["done"]), int(r["total"])) for r in progress_rows}
