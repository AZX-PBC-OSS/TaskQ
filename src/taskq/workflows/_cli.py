"""The ``taskq flows`` analysis engine (T12): one question, one surface.

Every function here is pure analysis — fetched rows in, report lines out.
Nothing in this module touches Typer, a connection, or any CLI state: the
asyncpg fetchers and the ``typer.echo`` rendering live in ``taskq.cli``
(the ``flows`` sub-app), which is this module's only importer. This is
``_doctor.py``'s seam, mirrored.

THE ONE-DERIVATION LAW: the run status reported here is THE §17.5
derivation (:func:`taskq.workflows._status.derive_workflow_status`) — the
CLI never re-derives, it converts the fetched rows into the derivation's
``NodeView`` inputs and reads the shared output. One engine, many
surfaces (backend, SSE, CLI — no mapping drift, PROPOSAL §13.4).

THE WHY-STUCK SEAM (G10): :func:`stuck_lines` is the shared analysis a
future ``taskq explain <job_id>`` calls for the per-node why-stuck arm —
``flows status`` CALLS it; it is never re-implemented per surface.

THE OUTPUT CONTRACT (the acceptance gates):
* every number names its evidence source (``_doctor.py``'s report style);
* every stuck finding names its REMEDY (the command that answers it);
* an empty snapshot prints the honest degraded line — never a blank
  section that reads as a healthy zero (#673's empty-snapshot rule).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, cast

from pydantic import BaseModel

from taskq._json import loads as _json_loads
from taskq.workflows._status import NodeView, WorkflowStatus, derive_workflow_status
from taskq.workflows.api._hitl import TIMER_SIGNAL_PREFIX, HoldContext, starts_with_timer

__all__ = [
    "FLOW_ROOT_STEP_KEY",
    "FlowListRow",
    "FlowNodeRow",
    "code_version_drift_lines",
    "derive_flow_status",
    "format_flow_list",
    "format_flow_status",
    "format_holds",
    "gate_models_for",
    "parse_decision",
    "stranded_lines",
    "stuck_lines",
]

#: The flow root row's step_key — the run's linearization point, never
#: counted among the nodes (the rollup's own clause spells it).
FLOW_ROOT_STEP_KEY: Final[str] = "__flow__"


@dataclass(frozen=True, slots=True)
class FlowNodeRow:
    """One node's fetched rollup row (``WORKFLOW_NODES_SQL``) + the hold
    shape resolved beside it (the held row's representation is the
    signal row — the node row alone cannot say WHAT it waits for)."""

    step_key: str
    status: str
    deps_pending: int = 0
    blocking_reason: str | None = None
    absorbed: bool = False
    error_class: str | None = None
    error_message: str | None = None
    hold: HoldContext | None = None
    max_attempts: int = 3
    attempt: int = 0

    def view(self) -> NodeView:
        """The §17.5 derivation's input — THE ONE CONVERSION (no second
        derivation lives anywhere)."""
        return NodeView(
            status=self.status,
            deps_pending=self.deps_pending,
            blocking_reason=self.blocking_reason,
            held=self.hold is not None,
            absorbed=self.absorbed,
        )


@dataclass(frozen=True, slots=True)
class FlowListRow:
    """One run's list row: the root row + the derived status."""

    run_id: str
    workflow: str
    root_status: str
    derived: WorkflowStatus
    nodes_total: int
    created_at: datetime | None


def derive_flow_status(nodes: Sequence[FlowNodeRow]) -> WorkflowStatus:
    """The run's status — THE derivation's output (never a re-derivation
    here; the row→NodeView conversion is the only CLI-side code)."""
    return derive_workflow_status(tuple(n.view() for n in nodes))


def _bounded_line(text: object, limit: int = 120) -> str:
    """THE DB-SOURCED TEXT'S DISCIPLINE (F-CLI-4's cure, the class seat):
    anything the RENDER surfaces print from a DB-sourced string is
    whitespace-COLLAPSED (embedded newlines cannot forge a remedy line —
    an attacker-shaped error_message or hold reason writing its own
    'remedy: …' line is the convicted forgery) and BOUNDED (the length
    cap the event-detail line already carries). The escape sequences
    die in the collapse: an ANSI escape's carriage moves and line feeds
    are whitespace like any other."""
    if text is None:
        return ""
    import re as _re

    # the ANSI escapes first (an ESC sequence is not whitespace — the
    # collapse alone would carry the coloring/erasing bytes to the tty);
    # then the control-character strip; then the whitespace collapse.
    text = _re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", str(text))
    text = "".join(ch for ch in text if ch == "\t" or ord(ch) >= 32)
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    remaining = len(text) - limit
    suffix = f"... (+{remaining} characters)"
    return text[: limit - len(suffix)] + suffix


def stuck_lines(node: FlowNodeRow, run_id: str) -> list[str]:
    """THE WHY-STUCK ARM (the explain seam): what this node is waiting
    on and the ONE command that answers it. A live (non-stuck) node
    yields an empty list — a running row is not a finding.

    THE BLOCKED-BEFORE-JOIN-WAIT ORDER (attack-4 F-P4-WHYSTUCK-FALSE-
    REMEDY's cure): a row whose parent ALREADY finalized as a failure
    carries ``blocking_reason='failed_parent'`` — the join can NEVER
    fire. Reading the join-wait arm first promised "the join fires when
    its parents finalize" for a join that never will; the blocking
    REASON wins the read, and each reason's remedy derives from the
    reason (truthful per state), never from the shape it resembles."""
    if node.hold is not None:
        # THE TIMER'S BRANCH (the timer-cures lane's cure 3 — the same
        # reserved-prefix split the template keeps): a hold under the
        # reserved ``__timer__`` prefix is the CLOCK's — the resolve
        # remedy is a lie the boundary would refuse. The line names the
        # timer AS a timer: the key, the scheduled instant, the wake's
        # own face (no resolve, no expiry fail-close — the wake fires).
        if starts_with_timer(node.hold.signal_name):
            timer_key = node.hold.signal_name[len(TIMER_SIGNAL_PREFIX) :] or "(anonymous)"
            scheduled = (
                f" · scheduled {node.hold.expires_at}"
                if node.hold.expires_at is not None
                else " · NO scheduled instant (a timer whose row carries no deadline — "
                "a hand-written row; the wake cannot fire)"
            )
            return [
                f"  {node.step_key}: HELD — waiting on: durable timer '{timer_key}'"
                f" · hold {node.hold.hold_id}{scheduled} "
                "(source: the wf_signals hold row's reserved __timer__ prefix)",
                "       remedy: none — the clock is the only resolver; the wake "
                "fires itself at/after the scheduled instant (the timer's "
                "deadline-pass is the continuation, never a fail-close)",
            ]
        deadline = (
            f" · deadline {node.hold.expires_at}"
            if node.hold.expires_at is not None
            else " · NO deadline (the W1 warning's subject — a workflow that waits "
            "forever on a human is a support ticket)"
        )
        lines = [
            f"  {node.step_key}: HELD — waiting for: signal '{node.hold.signal_name}'"
            f" · hold {node.hold.hold_id}{deadline}",
            f"       remedy: taskq flows resolve {node.hold.hold_id} '<decision json>'",
        ]
        if node.hold.reason:
            # THE WAITING-ON STATE's why (the author's declared reason —
            # the operator reads it without opening the code) — THROUGH
            # THE DISCIPLINE: a DB-sourced reason cannot forge a line.
            lines.insert(1, f"       reason: {_bounded_line(node.hold.reason)}")
        return lines
    if node.blocking_reason is not None and node.view().is_blocked_row:
        reason = node.blocking_reason
        if reason == "failed_parent":
            lines = [
                f"  {node.step_key}: BLOCKED — failed_parent "
                "(source: the row's metadata.blocking_reason stamp): a "
                "fail_closed parent finalized as a FAILURE — the join can "
                "never fire and this node can never run",
                f"       remedy: retry the failed upstream node — "
                f"taskq flows status {run_id} names it "
                "(or cancel the run: the path is dead)",
            ]
        else:
            lines = [
                f"  {node.step_key}: BLOCKED — {reason} "
                "(source: the row's metadata.blocking_reason stamp)",
                f"       remedy: retry the failed upstream node — "
                f"taskq flows status {run_id} names it",
            ]
        return lines
    if node.view().is_join_wait:
        return [
            f"  {node.step_key}: JOIN-WAIT — waiting on {node.deps_pending} upstream "
            "result(s) (source: the row's deps_pending counter, jobs.deps_pending)",
            "       remedy: none — the join fires when its parents finalize",
        ]
    if node.status == "failed":
        headroom = node.max_attempts - node.attempt
        headroom_note = (
            f" · ladder headroom {headroom} attempt(s) left"
            if headroom > 0
            else " · the ladder is EXHAUSTED (attempt = max_attempts)"
        )
        return [
            f"  {node.step_key}: FAILED — {_bounded_line(node.error_class or 'UnknownError', 60)}: "
            f"{_bounded_line(node.error_message)}{headroom_note} "
            "(source: the jobs row's error columns + attempt counters)",
            f"       remedy: taskq flows retry {run_id} {node.step_key}"
            "  (re-pends the node and re-opens its blocked closure)",
        ]
    return []


def _counts_by_status(nodes: Sequence[FlowNodeRow]) -> list[tuple[str, int]]:
    """The per-status counts, deterministic (status-sorted)."""
    counts: dict[str, int] = {}
    for n in nodes:
        counts[n.status] = counts.get(n.status, 0) + 1
    return sorted(counts.items())


def _registry_family_key(step_key: str) -> str:
    """The run node key's REGISTRY family (the fork's own shape): a
    map/fork child runs under ``<source>.item…`` — the CURRENT
    registry's compiled graph declares the SOURCE only (the children
    are runtime-forked, never compiled nodes). The family key is what
    the stranded-check compares."""
    if ".item" in step_key:
        return step_key.split(".item", 1)[0]
    return step_key


def stranded_lines(
    run_node_keys: Sequence[str],
    registry_node_keys: Sequence[str],
    *,
    workflow: str,
) -> list[str]:
    """THE VERSIONING WEDGE'S SURFACE (the timer-cures lane's cure 5 —
    the surface, not the semantics): the run's node inventory vs the
    CURRENT registry's compiled graph — a run node the current registry
    does not declare can never claim a body again, so an in-flight run
    at it is a ZOMBIE (never terminal, nothing fails it — the
    rename/remove redeploy wedge). The lines NAME the stranded nodes,
    state the zombie law AND the mid-map mixing read (a redeploy
    mid-map provably mixes v1+v2 results in one run — the
    registry-is-truth contract; the per-node ``jobs.code_version``
    stamps name which side each attempt ran), and give the remedy (the
    cancel-then-restart path §6 teaches).

    THE OUTPUT CONTRACT: every line names its evidence source (the run's
    node rows vs the --app module's compiled graph); the healthy zero is
    the EMPTY list (no line — a drift-free run renders no section)."""
    registry = set(registry_node_keys)
    stranded = sorted({key for key in run_node_keys if _registry_family_key(key) not in registry})
    if not stranded:
        return []
    shown = ", ".join(stranded[:8])
    if len(stranded) > 8:
        shown += f" … (+{len(stranded) - 8} more)"
    return [
        f"versioning drift: {len(stranded)} node(s) of this run are NOT in the "
        f"current registry's graph for workflow {workflow!r}: {shown} "
        "(source: the run's node rows vs the --app module's compiled graph)",
        "  a node the current registry does not declare can never claim a body "
        "again — an in-flight run stuck at it is a ZOMBIE: never terminal, "
        "nothing fails it (the rename/remove redeploy wedge — the guide's §6)",
        "  a mid-map redeploy also MIXES code versions in one run (the "
        "registry-is-truth contract): the per-node stamps (jobs.code_version) "
        "name which side of the drift each attempt ran",
        "  remedy: cancel the run — taskq flows cancel <run_id> — and re-run on "
        "the current code under a new run key",
    ]


def code_version_drift_lines(
    stamps: Sequence[tuple[str, str | None]],
) -> list[str]:
    """THE §22.1 STAMPS' DRIFT FACE (rv5-t5's cure — the surface, the
    stamps are the data): a run whose node rows carry >1 DISTINCT
    ``code_version`` was redeployed mid-flight — the run mixes pre- and
    post-deploy attempts, and the operator sees it HERE, from the run's
    OWN rows (no ``--app`` door needed: the stamps ride the rows).

    THE OUTPUT CONTRACT (the stranded face's own — one coherent drift
    section): every line names its evidence source (the run's node rows'
    ``jobs.code_version``); the multi-hash nodes are named WITH their
    hashes (a map family whose rows straddle the deploy names both
    sides); the healthy zero is the EMPTY list (a single-stamp run
    renders no section)."""
    by_key: dict[str, set[str]] = {}
    for step_key, version in stamps:
        if version is None:
            continue
        by_key.setdefault(step_key, set()).add(version)
    distinct = sorted({v for versions in by_key.values() for v in versions})
    if len(distinct) < 2:
        return []
    multi = sorted(key for key, versions in by_key.items() if len(versions) > 1)
    lines = [
        f"code-version drift: {len(distinct)} distinct stamps on this run's rows "
        f"(source: the node rows' jobs.code_version — the §22.1 record): "
        + " ".join(f"{v[:12]}…" for v in distinct[:4])
        + (" …" if len(distinct) > 4 else ""),
    ]
    if multi:
        shown = ", ".join(f"{key} ({'/'.join(sorted(by_key[key])[:3])})" for key in multi[:8])
        if len(multi) > 8:
            shown += f" … (+{len(multi) - 8} more)"
        lines.append(
            f"  multi-hash nodes: {shown} — a redeploy landed mid-family (the "
            "map's rows straddle the deploy)"
        )
    lines.append(
        "  the run mixes pre- and post-deploy attempts; if the mix matters, "
        "cancel + re-run on the current code under a new run key"
    )
    return lines


def format_flow_status(
    *,
    run_id: str,
    workflow: str,
    root_status: str,
    nodes: Sequence[FlowNodeRow],
    cancel_in_flight: bool = False,
    registry_node_keys: Sequence[str] | None = None,
    code_version_stamps: Sequence[tuple[str, str | None]] | None = None,
) -> list[str]:
    """`flows status <run_id>`'s report lines.

    The order is: the header (the run's identity + THE DERIVED status),
    the counts (with their source), the stuck nodes' why-stuck lines
    (the explain seam), the ONE coherent versioning-drift section — the
    stranded-node surface (only when *registry_node_keys* is given: the
    ``--app`` door) + the §22.1 stamps' drift face (when the run's rows
    carry >1 distinct ``code_version`` — the stamps are the data, no
    ``--app`` needed) — then the remedy footer. The root row's OWN
    status is reported as the cache it is (G7's law: the rows are the
    truth, the root row is a cache)."""
    derived = derive_flow_status(nodes) if nodes else "pending"
    lines = [
        f"run {run_id}",
        f"workflow: {workflow or '(unrecorded)'}",
        f"status: {derived}  (derived from the node rows — §17.5; "
        f"the root row caches {root_status!r})",
    ]
    if registry_node_keys is not None and nodes:
        lines.extend(
            stranded_lines(
                [n.step_key for n in nodes],
                registry_node_keys,
                workflow=workflow,
            )
        )
    if code_version_stamps:
        lines.extend(code_version_drift_lines(code_version_stamps))
    if cancel_in_flight:
        lines.append("cancel: IN FLIGHT (the row's cancel_requested_at is set)")
    if not nodes:
        # THE EMPTY SNAPSHOT (#673's rule): an honest degraded line, never
        # a blank section that reads as a healthy zero.
        lines.append(
            "nodes: none yet (source: jobs rows by metadata.flow_id — the run "
            "was created but its nodes are not inserted, or the id names no run)"
        )
        lines.append("       remedy: check the id — taskq flows list shows the recent runs")
        return lines
    counts = ", ".join(f"{c} {s}" for s, c in _counts_by_status(nodes))
    lines.append(f"nodes: {len(nodes)} — {counts} (source: jobs rows by metadata.flow_id)")
    # The stuck section is BOUNDED (the admin's timeline-truncation
    # convention): a 1000-child map's 3 failures name themselves; a
    # 500-child stuck map would otherwise flood the operator's terminal —
    # the remainder is counted, never silently dropped.
    stuck = [line for n in nodes for line in stuck_lines(n, run_id)]
    stuck_cap = 12
    lines.extend(stuck[:stuck_cap])
    if len(stuck) > stuck_cap:
        lines.append(
            f"  ... and {len(stuck) - stuck_cap} more stuck row(s) — narrow with "
            f"taskq flows holds {run_id} or the admin's run explorer"
        )
    if not stuck:
        lines.append("next: nothing is stuck — the run is progressing or terminal")
    return lines


def format_holds(holds: Sequence[HoldContext], *, run_id: str | None = None) -> list[str]:
    """`flows holds <run_id>`'s report lines — the operator's "what is
    this run waiting on" answer. THE EMPTY SNAPSHOT: a run with zero
    pending holds prints the honest line (a healthy zero is STATE — it
    must say so, not render blank)."""
    if not holds:
        where = f" for run {run_id}" if run_id else ""
        return [
            f"holds: none pending{where} (source: wf_signals rows with status = 'held')",
            "       that is a healthy zero — nothing in this run waits on a human",
        ]
    lines = [f"holds: {len(holds)} pending (source: wf_signals rows with status = 'held')"]
    for h in holds:
        deadline = f" · deadline {h.expires_at}" if h.expires_at is not None else " · no deadline"
        lines.append(
            f"  hold {h.hold_id} — gate {h.signal_name} · node {h.node_key}"
            f" · epoch {h.hold_epoch}{deadline}"
        )
        if h.reason:
            lines.append(f"    reason: {_bounded_line(h.reason)}")
        lines.append(f"    remedy: taskq flows resolve {h.hold_id} '<decision json>'")
    return lines


def format_flow_list(rows: Sequence[FlowListRow]) -> list[str]:
    """`flows list`'s report lines — the recent runs with their DERIVED
    statuses. THE EMPTY SNAPSHOT: no runs recorded yet → the honest line."""
    if not rows:
        return [
            "runs: none yet (source: jobs rows where step_key = '__flow__')",
            "      a fresh install or a schema with no workflow runs yet is not an "
            "error — trigger one from your app or the demo",
        ]
    lines = [f"runs: {len(rows)} (source: jobs rows where step_key = '__flow__', newest first)"]
    for r in rows:
        lines.append(
            f"  {r.run_id}  {r.derived:<10} root={r.root_status:<10} "
            f"nodes={r.nodes_total:<4} {r.workflow}  {r.created_at}"
        )
    return lines


def parse_decision(raw: str) -> dict[str, object]:
    """The resolve/signal payload's parse: a JSON object in, a dict out;
    anything else raises the named error the operator SEES (the caller
    turns it into exit 1 — the CLI's refusal shape, never a traceback)."""
    try:
        parsed = _json_loads(raw)
    except ValueError as exc:
        raise ValueError(f"the decision is not valid JSON: {exc}") from None
    if not isinstance(parsed, dict):
        raise ValueError(
            f"the decision must be a JSON object, got {type(parsed).__name__}: {raw[:80]}"
        )
    return cast(dict[str, object], parsed)


def gate_models_for(app: Any, workflow: str, node_key: str) -> tuple[type[BaseModel], ...]:
    """The bound gates' payload models for ONE node (the typed door's
    runtime half — T09's compile bound them at the gate; the CLI loads
    the app and reads the SAME registry, never a second one)."""
    compiled = app.get(workflow)
    decl = compiled.nodes[node_key]
    models: list[type[BaseModel]] = []
    for gate in decl.gates:
        models.extend(gate.payload_models)
    return tuple(models)
