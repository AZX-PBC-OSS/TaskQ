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


def _shape(node_kind: str, gated: bool) -> tuple[str, str]:
    """(open, close) delimiters per node kind — the §10.1 vocabulary."""
    if gated:
        return ("([(", ")])")  # the HOLD node — the HITL interrupt point
    if node_kind == "map_source":
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
        open_, close = _shape(node.kind, gated)
        gate_note = ""
        if gated:
            policies = ",".join(
                f"{g.name}{'/' + str(g.timeout_s) + 's' if g.timeout_s is not None else '/∞'}"
                for g in node.gates
            )
            gate_note = f" ⏳{policies}"
        lines.append(f'    {key}{open_}"{key}{glyph}{gate_note}"{close}')
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
