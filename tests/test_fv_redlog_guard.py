"""FV-DELIVERABLE — THE REDLOG LAW GUARD (the findings-validation round).

THE LAW (the new evidence law this round enforces): **a redlog entry is
machine-generated or it doesn't exist.**

The redlog sinks (``.measurements/*-pin-reds.json``, the mutation drills'
recorded reds) are append-only and run-scoped since the phase-3 fixer —
but the SINK MECHANICS were fixed while the FABRICATED ENTRIES survived:
several pins call ``redlog.red(pin, mutation, observed)`` with a
hand-written dict LITERAL describing an observation NO drill ever made in
that process (e.g. ``{"zombie_wake": True, ...}`` recorded at the end of a
test whose assertions prove the OPPOSITE). A red recorded from a literal
is prose wearing an evidence file's clothes — the exact class the
fabricated-red conviction named.

THE ENFORCEMENT: an ``observed`` payload built ONLY from syntactic
constants can never be evidence — a drill's record must DERIVE at least
one value from the run (a live DB read, a computed flag). This guard
AST-walks every test module and fails on any ``.red(...)`` call whose
third argument is a literal-only ``dict``/``set``/``tuple``/``list``
(composed entirely of ``ast.Constant`` nodes and literal-only
containers). Calls that derive any value from the run's own reads pass
(``{"executions_after_redelivery": executions}`` — the honest drill's
shape).

Failures name file:line + the pin — the fix is to RUN the mutation drill
and record what it actually produced (or delete the entry: a red that was
never run doesn't exist).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent

#: The redlog sinks' method name (the fixture handles: engine_redlog,
#: hitl_redlog, loop_redlog, ledger_redlog, propagation_redlog, t20_,
#: t21_, progress_redlog — every RedLog instance).
_RED_METHOD = "red"

_LITERAL_NODES = (ast.Constant,)


def _is_literal(node: ast.expr) -> bool:
    """True when the expression is composed ONLY of constants and
    literal-only containers — a hand-constructed payload, never a
    machine-generated observation."""
    if isinstance(node, _LITERAL_NODES):
        return True
    if isinstance(node, ast.Dict):
        return all(_is_literal(k) and _is_literal(v) for k, v in zip(node.keys, node.values))
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return all(_is_literal(e) for e in node.elts)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        return _is_literal(node.operand)
    return False


def _modules() -> list[Path]:
    return sorted(_TESTS.glob("test_*.py"))


def test_redlog_entries_are_machine_generated_not_hand_constructed() -> None:
    """THE LAW: every redlog .red() payload derives at least one value
    from the drill's own run. Literal-only payloads are fabricated
    observations — the red doesn't exist."""
    violations: list[str] = []
    for path in _modules():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != _RED_METHOD:
                continue
            if len(node.args) < 3:
                continue
            observed = node.args[2]
            if _is_literal(observed):
                pin = ""
                if isinstance(node.args[0], ast.Constant):
                    pin = str(node.args[0].value)
                violations.append(
                    f"{path.name}:{node.lineno} pin={pin!r}: the observed "
                    "payload is a hand-written literal — no drill produced "
                    "it. Run the mutation drill and record what it actually "
                    "yielded, or delete the entry (a red that never ran "
                    "doesn't exist)."
                )
    assert not violations, (
        "THE REDLOG LAW: a redlog entry is machine-generated or it doesn't "
        "exist — these .red() calls record hand-constructed payloads:\n"
        + "\n".join(violations)
    )


def test_guard_bites() -> None:
    """The guard's own red: a literal payload must be convicted, a
    derived payload must pass."""
    literal = ast.parse(
        "log.red('pin-x', 'mutation', {'zombie_wake': True, 'n': 3})"
    ).body[0]
    call = next(n for n in ast.walk(literal) if isinstance(n, ast.Call))
    assert isinstance(call.args[2], ast.expr)
    assert _is_literal(call.args[2]), "the literal payload must be convicted"

    derived = ast.parse(
        "log.red('pin-x', 'mutation', {'rows': rows, 'fired': rows == 0})"
    ).body[0]
    call_d = next(n for n in ast.walk(derived) if isinstance(n, ast.Call))
    assert not _is_literal(call_d.args[2]), "the derived payload must pass"
