"""The compile-time Mermaid emission (T09, §10.1) — a PURE function of the
wiring: same module → same Mermaid, byte-stable, golden-testable.

* edge labels = the promise types (the data that flows, spelled on the
  edge);
* node shapes by kind — stadium ``([])`` = map source, hexagon ``{{}}`` =
  collapsed map (the join), rectangle ``[]`` = plain actor, ``[[(...)]]
  ``-shaped ``[(...)]`` = the HITL interrupt points (the declared gates,
  rendered with their signal types + timeout policy — T10's gate consumes
  the same declaration);
* deterministic order: nodes and edges iterate in SORTED key order, so
  the emission never depends on dict insertion order (the byte-stability
  law).

THE DIAGRAM-LIES PROPERTY (the acceptance gate): every node/edge in the
compiled graph appears in the emission and nothing else does — the
property test re-derives the emitted nodes/edges from the graph object
and compares. One source of truth for both.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskq.workflows.api._app import CompiledWorkflow

__all__ = ["render_mermaid"]


def _body_name(body: object) -> str:
    """The body's short name for the label (an unreadable body — a
    partial, a callable object — renders the type's name; the emission
    never crashes on a declaration's shape)."""
    name = getattr(body, "__name__", None)
    if isinstance(name, str):
        return name
    return type(body).__name__


def _shape(node_kind: str, gated: bool) -> tuple[str, str]:
    """(open, close) delimiters per node kind — the §10.1 vocabulary."""
    if gated:
        return ("([(", ")])")  # the HOLD node — the HITL interrupt point
    if node_kind in ("map_source", "route_source"):
        return ("([", "])")
    if node_kind == "map_join":
        return ("{{", "}}")
    if node_kind == "gather":
        return ("[[", "]]")
    return ("[", "]")


def _label_of(data_type: object) -> str:
    """The promise type's short label (the edge label)."""
    if data_type is None:
        return "?"
    name = str(data_type)
    # ``<class 'x.Y'>`` → ``Y``; ``list[<class 'x.Y'>]`` stays readable.
    if name.startswith("<class") and "'" in name:
        return name.split("'")[1].rsplit(".", 1)[-1]
    return name.replace("typing.", "")


def render_mermaid(compiled: CompiledWorkflow) -> str:
    """The emission — sorted, byte-stable, pure."""
    lines: list[str] = ["flowchart TD"]
    for key in sorted(compiled.nodes):
        node = compiled.nodes[key]
        gated = bool(node.gates)
        glyph = " ⇅" if gated else ""
        # THE FORK-CARRYING NODE'S SHAPE (the rv4 cure — the Mermaid
        # arms' render): a node carrying the plain map's item body or
        # the typed route's arms IS a map source — it finalizes into
        # fork children the rows will name. The pre-cure render drew it
        # as the plain rectangle (the route's branches invisible — the
        # diagram lied about the fork the wiring declared); the stadium
        # + the arms ON the label render the branches the compile
        # carries (byte-stable: sorted tags, declared body names).
        is_map_source = node.map_item is not None or node.map_arms is not None
        open_, close = _shape("route_source" if is_map_source else node.kind, gated)
        gate_note = ""
        if gated:
            policies = ",".join(
                f"{g.name}{'/' + str(g.timeout_s) + 's' if g.timeout_s is not None else '/∞'}"
                for g in node.gates
            )
            gate_note = f" ⏳{policies}"
        arms_note = ""
        if node.map_arms is not None:
            arms = ", ".join(
                f"{tag.rsplit('.', 1)[-1]}→{_body_name(arm.body)}"
                for tag, arm in sorted(node.map_arms.items())
            )
            arms_note = f" ⇢ route: {arms}"
        elif node.map_item is not None:
            arms_note = f" ⇢ map: {_body_name(node.map_item)}"
        lines.append(f'    {key}{open_}"{key}{glyph}{gate_note}{arms_note}"{close}')
    edges: list[str] = []
    for key in sorted(compiled.nodes):
        node = compiled.nodes[key]
        for parent in node.parents:
            if parent not in compiled.nodes:
                continue  # the E3 refusal's subject, not the render's lie
            label = _label_of(
                compiled.nodes[parent].body.__annotations__.get("return")
                if compiled.nodes[parent].body is not None
                else None
            )
            edges.append(f"    {parent} -->|{label}| {key}")
    lines.extend(sorted(edges))
    return "\n".join(lines) + "\n"
