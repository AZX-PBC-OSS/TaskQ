"""ATTACK4 — the NEW surfaces' negative type-probes (the phase-4
attacker's corpus): the admin's run-explorer + the CLI's analysis module
under BOTH pinned checkers. Each MUST_ERROR marker names the rule-ids
the violation must red under (the gate's vocabulary). Run:

    cd tests/typeprobe && ../../.venv/bin/pyright --project pyrightconfig.json attack4_new_surfaces_negative_types.py
    ../../.venv/bin/ty check attack4_new_surfaces_negative_types.py
"""

from __future__ import annotations

import uuid

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
    row = FlowNodeRow(  # MUST_ERROR(reportGeneralTypeIssues, unresolved-attribute, invalid-argument-type)
        step_key="review",
        status="pending",
        nonexistant_field=1,
    )
    print(row)

    # The report kwarg's typo: a keyword the function does not declare.
    lines = (
        format_flow_status(  # MUST_ERROR(reportCallIssue, invalid-argument-type, unknown-argument)
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
    ]  # MUST_ERROR(reportIndexIssue, unsupported-operator, invalid-assignment-target)
    print(first)


def probe_the_gate_decl() -> None:
    # A GateDecl field that does not exist: the wiring's typo door.
    gate = GateDecl(  # MUST_ERROR(reportCallIssue, unknown-argument, invalid-argument-type)
        name="Approval",
        payload_models=(Approval,),
        timeout_s=120.0,
        nonexistent_kwarg=True,
    )
    print(gate)


def probe_the_uuid_boundary(value: str) -> None:
    # A str where the run explorer's uuid.UUID param is declared.
    uuid.UUID(value, version=None)  # MUST_ERROR(reportArgumentType, invalid-argument-type)
