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

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from taskq.backend._protocol import ConnLike
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import NodeView, derive_workflow_status

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
    "error_class, error_message, metadata->>'workflow' AS workflow "
    "FROM \"{schema}\".jobs WHERE id = $1 AND step_key = '__flow__'"
)

# The run's edge read (the graph's edge set): COLLAPSED TO KEY PAIRS IN
# SQL (Q1d's read-side bound — the join's incoming edges are ONE PER
# CHILD, so a 10k fan-out carries 10k rows into every render/SSE poll
# when read by id). The re-point: an edge whose PARENT is itself a
# collapsed map child (a fan-out child feeding the fan-in join — the
# fork's own edge shape) renders as source → join (the hexagon feeds
# the join, the graph's shape preserved); the discriminator is the
# joined-row family's blocking_reason marker (never NULL on a join row,
# never set on a fan-out child), not a step-key string convention (pin
# 18's dragon). DISTINCT bounds the read to the graph's real edge set.
_RUN_EDGES_SQL = """\
SELECT DISTINCT
       COALESCE(src.step_key, p.step_key) AS parent_key,
       c.step_key AS child_key
FROM "{schema}".wf_edge e
JOIN "{schema}".jobs p ON p.id = e.parent_id
LEFT JOIN "{schema}".jobs src ON src.id = p.parent_id
  AND p.metadata->>'blocking_reason' IS NULL
JOIN "{schema}".jobs c ON c.id = e.child_id
WHERE e.child_id = ANY($1)
"""

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
    # THE COLLAPSE (Q1d's cure): this row is a fork/map CHILD of another
    # node in this run (its parent_id names a node row of the same run) —
    # the renderers COLLAPSE it into its parent source's hexagon (the
    # parent carries the done/total counter; the child detail lives in
    # the drill-down). The derivation reads EVERY row — the child's
    # status drives the §17.5 verdict exactly as before; only the RENDER
    # collapses (a 10k fan-out renders ONE hexagon, never 10k lines).
    map_child: bool = False

    def view(self) -> NodeView:
        """The §17.5 derivation's input (the one conversion — the same
        law the CLI's analysis module states)."""
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
    cancel_requested_at: str | None
    error_class: str | None
    error_message: str | None
    nodes: list[RunNode]
    holds: list[dict[str, Any]]
    derived: str = ""
    # THE ROOT'S MAP COUNTER (Q1d's cure): the SUM across the run's map
    # sources — the run-level face of the collapse (the hexagons carry
    # the per-source counts; this is the run's total fan-out). Zero for
    # a run with no map/fork fan-out.
    root_map_done: int = 0
    root_map_children: int = 0

    def derive(self) -> str:
        """THE derivation's output, cached onto the view (the report's
        status line and the G7 check read the same value)."""
        if not self.derived:
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
    wsql = WorkflowSql.build(schema)
    root = await conn.fetchrow(_RUN_ROOT_SQL.format(schema=schema), run_id)
    if root is None:
        return None
    node_rows = await conn.fetch(wsql.workflow_nodes, run_id)
    node_ids = [r["id"] for r in node_rows]
    edge_rows = await conn.fetch(_RUN_EDGES_SQL.format(schema=schema), node_ids) if node_ids else []
    hold_rows = await conn.fetch(_RUN_HOLDS_SQL.format(schema=schema), run_id)
    # THE MAP PROGRESS READS THE TRUE PARENTS (Q1d's cure): the children
    # group by the map SOURCE they were born parented at (the fork/emit
    # INSERT binds the source's job id) — the read passes THE RUN'S NODE
    # IDS, never the root's (the root parents nothing: the old read fed
    # the root's id to ``parent_id = $1`` and every counter read zero —
    # the hexagon could never fire, and a 10k fan-out rendered 10k raw
    # node lines).
    progress = (
        summarize_map_children(
            [dict(r) for r in await conn.fetch(wsql.workflow_map_progress_sources, node_ids)]
        )
        if node_ids
        else {}
    )
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
    """The assembly: the rows → the view (the edges arrive as COLLAPSED
    key pairs — the edge read re-points a fan-out child's edge to its
    source; the held marker joins the signal rows; the map children
    collapse into their source's counter — the RENDER collapses, the
    derivation reads every row)."""
    node_ids = {r["id"] for r in node_rows}

    # THE COLLAPSED CHILDREN (Q1d): a row parented at ANOTHER node of
    # this run is the fan-out's child — the source node carries the
    # hexagon (map_done/map_children from the per-source progress read
    # keyed by the source's step_key); the child row itself collapses
    # out of every render. THE JOIN ROW IS NOT A CHILD: the fan-in is
    # born parented at the source too, but it is a NODE — the
    # joined-row family's blocking_reason marker discriminates (never
    # NULL on a join row, never set on a fan-out child). Static nodes
    # are parented at NULL.
    def _is_map_child(r: Mapping[str, Any]) -> bool:
        return (
            r["parent_id"] is not None
            and r["parent_id"] in node_ids
            and r["blocking_reason"] is None
        )

    collapsed_keys = {r["step_key"] for r in node_rows if _is_map_child(r)}
    parents_of: dict[str, list[str]] = {}
    for e in edge_rows:
        parent_key = e["parent_key"]
        child_key = e["child_key"]
        if parent_key == child_key or child_key in collapsed_keys:
            # A collapse-induced self-loop renders no edge; a collapsed
            # child's own incoming edge (source → child) renders none —
            # the hexagon speaks for it.
            continue
        parents_of.setdefault(child_key, []).append(parent_key)
    holds = [_normalize_hold(h) for h in hold_rows]
    held_by_node = {h["node_key"]: h for h in holds if h["status"] == "held"}
    nodes: list[RunNode] = []
    for r in node_rows:
        is_map_child = _is_map_child(r)
        step_key = r["step_key"]
        done, total = progress.get(step_key, (0, 0))
        nodes.append(
            RunNode(
                key=step_key,
                status=r["status"],
                deps_pending=r["deps_pending"],
                blocking_reason=r["blocking_reason"],
                absorbed=r["absorbed"],
                error_class=r["error_class"],
                error_message=r["error_message"],
                map_children=0 if is_map_child else total,
                map_done=0 if is_map_child else done,
                hold=dict(held_by_node[step_key]) if step_key in held_by_node else None,
                parents=tuple(sorted(set(parents_of.get(step_key, [])) - collapsed_keys)),
                map_child=is_map_child,
            )
        )
    return RunView(
        run_id=str(root["id"]),
        workflow=root["workflow"] or root["actor"],
        root_status=root["status"],
        created_at=_iso(root["created_at"]),
        finished_at=_iso(root["finished_at"]),
        cancel_requested_at=_iso(root["cancel_requested_at"]),
        error_class=root["error_class"],
        error_message=root["error_message"],
        nodes=nodes,
        holds=holds,
        root_map_done=sum(n.map_done for n in nodes if not n.map_child),
        root_map_children=sum(n.map_children for n in nodes if not n.map_child),
    )


def rows_mermaid(view: RunView) -> str:
    """The run graph's Mermaid text FROM THE ROWS — the compile's shape
    vocabulary, the run's live edge set. Byte-stable for identical rows
    (sorted emission, the compile's own law). A node the map collapsed
    renders as the hexagon-with-counter shape ({{...}}); ZERO child
    boxes — the child detail lives in the paginated panel. THE COLLAPSE
    IS THE BOUND (Q1d): the map/fork children (a row parented at another
    run node) render ZERO lines — their parent source's hexagon carries
    the done/total counter — so a 10k fan-out renders ONE hexagon, never
    10k raw node lines."""
    lines: list[str] = ["flowchart TD"]
    for node in sorted(view.nodes, key=lambda n: n.key):
        if node.map_child:
            # THE COLLAPSED CHILD: the parent source's hexagon speaks for
            # it (the counter is the per-source read, computed in SQL).
            continue
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
        if node.map_child:
            # The collapsed child's edges collapse with it (an edge to a
            # node that renders no line would mint an implicit mermaid
            # node — the amplifier's back door).
            continue
        for parent in sorted(node.parents):
            edges.append(f"    {parent} --> {node.key}")
    lines.extend(sorted(edges))
    return "\n".join(lines) + "\n"


def run_state_json(view: RunView, *, seq: int) -> dict[str, Any]:
    """The SSE snapshot's payload: the DERIVED status + every RENDERED
    node's state (the client patches by ``data-node-key``; a full
    replacement — the reconnect's replay IS this snapshot, so a killed
    connection never loses state). The map children collapse OUT of the
    frames (Q1d's client half): a 10k fan-out carries the source
    hexagon's counts + the run's summed root counter, never 10k node
    entries per frame — the SSE poll is the render cadence, the frame
    must stay bounded."""
    rendered = [n for n in view.nodes if not n.map_child]
    return {
        "seq": seq,
        "run_id": view.run_id,
        "status": view.derive(),
        "root_status": view.root_status,
        # THE ROOT'S MAP COUNTER: the fan-out's run-level face (the sum
        # across sources) — the page header renders it, the JS patches it.
        "map_done": view.root_map_done,
        "map_children": view.root_map_children,
        "nodes": [
            {
                "key": n.key,
                "status": n.status,
                "hold": n.hold is not None,
                "map_done": n.map_done,
                "map_children": n.map_children,
            }
            for n in sorted(rendered, key=lambda n: n.key)
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
    """The per-map-SOURCE done/total pairs from
    ``WORKFLOW_MAP_PROGRESS_SOURCES_SQL``'s rows (the collapsed hexagon's
    counter — one read, no second instrument), keyed by the SOURCE
    node's step_key: the children's TRUE parent (Q1d's cure), not the
    child step vocabulary."""
    return {r["source_key"]: (int(r["done"]), int(r["total"])) for r in progress_rows}
