"""ATTACK4 — the NEW surfaces' negative type-probes (the phase-4
attacker's corpus): the admin's run-explorer + the CLI's analysis module
under BOTH pinned checkers. Each ``MUST_ERROR(rules...)`` marker names
the rule-ids the violation must red under, in BOTH checkers'
vocabularies (the gate's law). THE EVIDENCE-INTEGRITY ROUND'S REPAIR
(wire-or-delete, no zombie corpus): this file ran only under the
attacker's own invocation and ROTTED — the markers declared pyright
rule-ids that no longer exist on the shipped checkers
(``reportGeneralTypeIssues``, ``reportIndexIssue``) while the real rules
moved; and the uuid-boundary probe was DEAD ON ARRIVAL (``uuid.UUID``
ACCEPTS a ``version=None`` str — no checker could ever red it; deleted).
Wired into ``_gate.py``'s corpus. Run:

    cd tests/typeprobe && ../../.venv/bin/pyright --project pyrightconfig.json attack4_new_surfaces_negative_types.py
    ../../.venv/bin/ty check attack4_new_surfaces_negative_types.py
"""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows._cli import (
    FlowNodeRow,
    format_flow_list,
    format_flow_status,
    parse_decision,
)
from taskq.workflows.api import GateDecl


class Approval(BaseModel):
    verdict: str


def probe_the_cli_rows() -> None:
    # A nonexistent field on the frozen row dataclass: the checker owns
    # the typo (the runtime would raise TypeError only at call time).
    row = FlowNodeRow(  # MUST_ERROR(reportCallIssue, unknown-argument)
        step_key="review",
        status="pending",
        nonexistant_field=1,
    )
    print(row)

    # The report kwarg's type: the cancel flag is bool, never a str.
    lines = (
        format_flow_status(  # MUST_ERROR(reportArgumentType, invalid-argument-type)
            run_id="r",
            workflow="w",
            root_status="running",
            nodes=[],
            cancel_in_flight="yes-not-a-bool",
        )
    )
    print(lines)

    # The list formatter fed a NON-sequence row type.
    format_flow_list([None])  # MUST_ERROR(reportArgumentType, invalid-argument-type)


def probe_the_parse_boundary() -> None:
    parsed = parse_decision('{"verdict": "approve"}')
    # The parse's return is a dict — indexing it as a list is a type error.
    first = parsed[
        0
    ]  # MUST_ERROR(reportArgumentType, invalid-argument-type): dict[str, object]'s __getitem__ takes str, never an int
    print(first)


def probe_the_gate_decl() -> None:
    # A GateDecl field that does not exist: the wiring's typo door.
    gate = GateDecl(  # MUST_ERROR(reportCallIssue, unknown-argument)
        name="Approval",
        payload_models=(Approval,),
        timeout_s=120.0,
        nonexistent_kwarg=True,
    )
    print(gate)
