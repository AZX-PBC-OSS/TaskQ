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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, cast

from taskq._humantime import split_seconds
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
    "error_class, error_message, metadata->>'workflow' AS workflow, "
    "metadata->>'parent_flow_id' AS parent_flow_id "
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

#: THE CHILD FLOWS' LINKS (rv5-C5b — the admin is not child-blind): the
#: run's child-flow receipts (the parent's node rows' ``child_flows``),
#: dedup'd and bounded (a 10k fan-out renders the count, the first ids
#: capped by the display bound the runs list carries).
_RUN_CHILD_FLOWS_SQL = (
    "SELECT DISTINCT (e.val)::text AS child_id "
    'FROM "{schema}".jobs j '
    "CROSS JOIN LATERAL jsonb_array_elements_text(j.metadata->'child_flows') AS e(val) "
    "WHERE (j.metadata->>'flow_id')::uuid = $1 AND j.metadata ? 'child_flows' "
    "ORDER BY child_id LIMIT $2"
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
    # THE DEFERRAL FACES (the run page's "why is this step sitting here"
    # answer): the row's attempt ordinal (the stamp's discriminator), the
    # disposition's reschedule (the next fire time), the row's deferral
    # counter, and the deferral stamp itself (the signal + the budget
    # face + the disposition's own post-attempt ordinal, written by
    # NODE_DEFER_SQL).
    attempt: int = 0
    scheduled_at: str | None = None
    snooze_count: int = 0
    deferral: dict[str, Any] | None = None
    # THE DEADLINE FACES (the wf start_to_close cure): the row's own
    # declared bound (the column's seconds) + the claim's instant (the
    # running face's age source) — the node panel's deadline line's
    # inputs. ``None`` = nothing declared (no line — the row is the
    # declaration's own truth).
    start_to_close_s: float | None = None
    started_at: str | None = None
    # THE CLOCK ARBITER (the single-arbiter doctrine — the label reads
    # the DATABASE's clock, ``running_for_ms``'s own law): the
    # deadline's remaining/age computed IN THE READ against
    # ``clock_timestamp()`` — never the app clock's guess (a DB ahead of
    # the app read as "expired 120s ago" on a step that just started).
    # Positive = the time left; negative = the expiry's age; ``None`` =
    # the face doesn't render the reality (the bound alone).
    deadline_remaining_s: float | None = None

    @property
    def deadline_label(self) -> str | None:
        """THE DEADLINE LINE (the page's and the SSE frame's shared
        spelling — one derivation, two renderers, the deferral label's
        own pattern): the bound WHERE DECLARED, and — for a RUNNING node
        whose claim instant is available — the deadline's REALITY (the
        instant + the bound, with the time left or the expiry's age).
        Sub-second spans — a bound under a second, a remaining under a
        second — read ``<1s`` (``0s`` reads as no time at all). ``None``
        = no bound declared (the line renders nothing — an
        undeclared row reads as the unbounded face it is). The REALITY's
        remaining is the READ's own computation (the DB clock's — the
        row's ``deadline_remaining_s``), never an app-clock guess."""
        if self.start_to_close_s is None:
            return None
        bound = _format_deadline_bound(self.start_to_close_s)
        if self.status != "running" or self.started_at is None:
            return f"deadline {bound}"
        remaining = self.deadline_remaining_s
        if remaining is None:
            return f"deadline {bound}"
        if remaining > 0:
            # THE SUB-SECOND FLOOR (the bound's own spelling): a remaining
            # under a second reads ``<1s`` — ``0s left`` reads as no time
            # at all on a clock that has not expired.
            left = f"{int(remaining)}s" if remaining >= 1 else "<1s"
            return f"deadline {bound} · {left} left"
        age = f"{int(-remaining)}s" if -remaining >= 1 else "<1s"
        return f"deadline {bound} · expired {age} ago"

    @property
    def deferral_label(self) -> str | None:
        """The SITTING state's one-line answer (the page's and the SSE
        frame's shared spelling — one derivation, two renderers), None
        when the node is not sitting on a deferral.

        THE ROW'S TRUTH, not a stale decoration: the label renders only
        while the row still sits at THIS deferral's own reschedule —
        status ``scheduled`` AND the row's attempt ordinal equal to the
        stamp's own post-disposition ordinal (written by the statement).
        A later ladder repend — the one other ``scheduled`` writer, at a
        strictly greater attempt — never reads as a live deferral; the
        awaited attempt's story stays on the ledger (the panel's
        timeline), where the record's truth lives."""
        if self.deferral is None or self.status != "scheduled":
            return None
        try:
            stamped = int(self.deferral.get("attempt", -1))
        except (TypeError, ValueError):
            return None
        if stamped != self.attempt:
            return None
        # THE SIGNAL'S BOUND (C3 — H-C9's law at the deferral's face): the
        # stamp is a row-sourced jsonb doc and its ``signal`` is free text
        # — a hostile write rides it into the label. The panel's own
        # error-text bound applies (the same truncation the class/
        # message fields carry): the label is a display, never a
        # megabyte line through the DOM and every SSE frame.
        signal = _truncate_error_text(str(self.deferral.get("signal") or "Snooze"))
        if self.deferral.get("consume_budget"):
            # THE CONSUMING FACE (the vanilla's own vocabulary): the burn
            # is the budget's charge — no snooze count (the counter never
            # moved), the budget face named.
            return f"⏰ {signal} (consumes budget)" + (
                f" · next fire {self.scheduled_at}" if self.scheduled_at else ""
            )
        return (
            f"⏰ {signal}"
            + (f" · next fire {self.scheduled_at}" if self.scheduled_at else "")
            + f" · snooze #{self.snooze_count}"
        )

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
    # THE PARENT↔CHILD LINKS (rv5-C5b's cure — the admin is not
    # child-blind): the run's own child-flow receipts (the parent's
    # page links each child's run page) and the run's OWN parent (a
    # child flow's page links back). Both are ROW facts — the identity
    # faces the child line landed, surfaced.
    child_flows: tuple[str, ...] = ()
    parent_flow_id: str | None = None

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
    and a UUID in them is the TypeError the frame must never carry.

    THE TIMER'S BRANCH (the timer-cures lane's cure 3): a hold under
    the reserved ``__timer__`` prefix IS a timer — the enrichment names
    it so (``is_timer``, the stripped ``timer_key``, and the wake's
    ``fired_by`` once delivered) so the renderers branch on the ROW's
    own truth: a timer renders AS a timer (the key, the scheduled
    instant, the clock as the resolver), never as a human decision
    with a Resolve form the boundary would refuse."""
    out = dict(row)
    for key, value in out.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        elif isinstance(value, uuid.UUID):
            out[key] = str(value)
    from taskq.workflows.api._hitl import TIMER_SIGNAL_PREFIX, starts_with_timer

    signal_name = out.get("signal_name")
    is_timer = isinstance(signal_name, str) and starts_with_timer(signal_name)
    out["is_timer"] = is_timer
    out["timer_key"] = str(signal_name)[len(TIMER_SIGNAL_PREFIX) :] if is_timer else None
    out["timer_fired_by"] = None
    if is_timer:
        payload = out.get("payload")
        if isinstance(payload, dict) and isinstance(payload.get("fired_by"), str):
            out["timer_fired_by"] = payload["fired_by"]
    return out


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _format_deadline_bound(seconds: float) -> str:
    """The declared bound's operator form: a sub-second bound reads
    ``<1s`` (a 0.25s deadline rendered ``0s`` reads as NO bound at all —
    the useless face; the whole-seconds round is the lie in the other
    direction), sub-minute bounds keep seconds resolution (``30s`` —
    ``<1m`` would lie), minutes and hours carry the two-unit split over
    ``split_seconds`` (the unit cascade's ONE home — the split is never
    re-derived here), the panel's own resolution."""
    if seconds < 1:
        return "<1s"
    days, hours, minutes, secs = split_seconds(round(seconds))
    if days:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    if minutes:
        return f"{minutes}m {secs}s" if secs else f"{minutes}m"
    return f"{secs}s"


def _decode_stamp(raw: Any) -> dict[str, Any] | None:
    """The deferral stamp's decode (asyncpg hands a str for the bare
    ``->`` operator on un-coded connections, a dict for typed paths)
    through the estate's JSON seam; a non-object — a corrupted or hostile
    write — reads None (the label is a display, never load-bearing). The
    cast is the JSONB boundary's declared contract (RFC 8259: the wire's
    object keys are strings)."""
    if isinstance(raw, dict):
        return cast("dict[str, Any]", raw)
    if not isinstance(raw, str):
        return None
    try:
        decoded: object = _json_loads(raw)
    except ValueError:
        return None
    return cast("dict[str, Any]", decoded) if isinstance(decoded, dict) else None


def _start_to_close_seconds(raw: Any) -> float | None:
    """The declared bound's seconds (the wf start_to_close cure): the
    interval column decodes to a :class:`timedelta` on coded
    connections — a str (an un-coded connection's ISO interval form) or
    anything else degrades to None (the line is a display, never
    load-bearing — an unreadable bound renders no line, never a
    500)."""
    if isinstance(raw, timedelta):
        return raw.total_seconds()
    return None


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
    view = _run_view_from_rows(
        dict(root),
        [dict(r) for r in node_rows],
        [dict(r) for r in edge_rows],
        [dict(r) for r in hold_rows],
        progress,
    )
    # THE PARENT↔CHILD LINKS (rv5-C5b): the receipts' read rides the
    # SAME grouped read (one connection, the page's own query budget).
    child_rows = await conn.fetch(
        _RUN_CHILD_FLOWS_SQL.format(schema=schema), run_id, _CHILD_LINKS_DISPLAY_LIMIT
    )
    view.child_flows = tuple(str(r["child_id"]) for r in child_rows)
    if root["parent_flow_id"] is not None:
        view.parent_flow_id = str(root["parent_flow_id"])
    return view


#: The child-links' display bound (the runs list's pagination idiom: a
#: 10k fan-out renders the first ids; the count speaks past the cap).
_CHILD_LINKS_DISPLAY_LIMIT: int = 50


#: The display bound the jobs page's Data section carries (the same
#: number): the node panel's error fields are row-sourced and a row can
#: carry megabytes of failure text — the panel serves the bounded form
#: with the remaining-count marker, truncation distinguishable from an
#: actually-small value. ``error_class`` rides the SAME law (the rv2
#: finding's cure): the class field is row-sourced free text too — a
#: hostile DB write's 200KB class served whole through the view and the
#: SSE snapshot while the sibling fields were bounded.
_TRACEBACK_DISPLAY_LIMIT: int = 2000


def _truncate_error_text(text: str | None) -> str | None:
    """The node panel's error-field bound (the jobs page's own
    ``_truncate_traceback`` convention, re-derived here — the import law
    keeps the module free of the jobs-page import)."""
    if text is None:
        return None
    if len(text) <= _TRACEBACK_DISPLAY_LIMIT:
        return text
    remaining = len(text) - _TRACEBACK_DISPLAY_LIMIT
    suffix = f"... ({remaining} more characters)"
    return text[: _TRACEBACK_DISPLAY_LIMIT - len(suffix)] + suffix


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
                error_class=_truncate_error_text(r["error_class"]),
                error_message=_truncate_error_text(r["error_message"]),
                map_children=0 if is_map_child else total,
                map_done=0 if is_map_child else done,
                hold=dict(held_by_node[step_key]) if step_key in held_by_node else None,
                parents=tuple(sorted(set(parents_of.get(step_key, [])) - collapsed_keys)),
                map_child=is_map_child,
                attempt=int(r["attempt"] or 0),
                scheduled_at=_iso(r["scheduled_at"]),
                snooze_count=int(r["snooze_count"] or 0),
                # THE DEFERRAL STAMP'S DECODE (the jsonb boundary's own
                # shape — the bare -> operator hands a str, a typed path
                # a dict): a corrupted stamp reads None (the label is a
                # display, never load-bearing).
                deferral=_decode_stamp(r["deferral"]),
                # THE DEADLINE FACES (the wf start_to_close cure): the
                # row's own bound (the interval's decode is the
                # timedelta the column carries on coded connections) +
                # the claim's instant. The read is TOLERANT of the
                # hand-built row shapes (the pins' seeds — a row dict
                # predating the column reads None, the absent face).
                start_to_close_s=_start_to_close_seconds(r.get("start_to_close")),
                started_at=_iso(r.get("started_at")),
                # THE CLOCK ARBITER: the remaining is the
                # READ's own clock_timestamp() computation — the label
                # never measures the DB-written instant against the app
                # clock. A row dict predating the column reads None (the
                # bound-alone face).
                deadline_remaining_s=(
                    float(r["deadline_remaining_s"])
                    if r.get("deadline_remaining_s") is not None
                    else None
                ),
            )
        )
    return RunView(
        run_id=str(root["id"]),
        workflow=root["workflow"] or root["actor"],
        root_status=root["status"],
        created_at=_iso(root["created_at"]),
        finished_at=_iso(root["finished_at"]),
        cancel_requested_at=_iso(root["cancel_requested_at"]),
        error_class=_truncate_error_text(root["error_class"]),
        error_message=_truncate_error_text(root["error_message"]),
        nodes=nodes,
        holds=holds,
        root_map_done=sum(n.map_done for n in nodes if not n.map_child),
        root_map_children=sum(n.map_children for n in nodes if not n.map_child),
    )


def _mermaid_label(text: str) -> str:
    """THE LABEL ESCAPE (H-C8's rows-side defense): a ROW-sourced key
    (pre-dating the compile's key charset law, or written by a hostile
    hand) can carry a raw ``"`` — inside the quoted Mermaid label that
    KILLS the render — or a raw NEWLINE — which injects a phantom line
    into the graph's text. The escape is byte-stable and touchless for
    clean keys (only the hostile bytes rewrite). The compile law (the
    key charset) is the build-side half; this is the rows-side half —
    the rows are the law, and the render must survive them."""
    return text.replace("\\", "\\\\").replace('"', "&quot;").replace("\r", " ").replace("\n", " ")


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
        label = _mermaid_label(node.key)
        if node.hold is not None:
            label += " ⏳"
        if collapsed:
            label += f" {node.map_done}/{node.map_children}"
        # THE LABEL IS ESCAPED (H-C8): the quote/newline-bearing hostile
        # row's label cannot kill the render or inject a line.
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
                # THE ERROR COLUMN'S CLASS (H-C9): the bounded error
                # class rides the frame — a node that fails while you
                # watch shows its class without a reload (the Error
                # column patches live, the same snapshot the chips
                # patch from).
                "error_class": n.error_class,
                "hold": n.hold is not None,
                "map_done": n.map_done,
                "map_children": n.map_children,
                # THE DEFERRAL LINE (the snoozed step's sitting state):
                # the label is the row's truth (the stamp's own
                # discriminator) — the JS paints/clears the line from
                # the frame, so a node that ARRIVES at (or leaves) the
                # snoozed state never needs a reload.
                "deferral_label": n.deferral_label,
                # THE DEADLINE LINE (the wf start_to_close cure): the
                # bound where declared + the running face's reality —
                # the same two-renderers pattern the deferral line
                # keeps.
                "deadline_label": n.deadline_label,
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
