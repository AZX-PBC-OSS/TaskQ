"""NEGATIVE TYPE PROBES — the T20 router's route-map typing (pyright
1.1.414 + ty 0.0.85), the §7b FORK-TYPED cure.

The same marker convention as the corpus's other files: each
``MUST_ERROR`` marker names the wrong-shape call the typed-doors law
requires to be a checker error, PLUS the exact rule-ids the pinned
checkers must emit.

THE TWO FACES, STATED HONESTLY (the probe file is the type story's
truth table for the router):

* FACE 1 — the CHECKER (this file): the route map's KEY and VALUE TYPES
  are enforced at the call site — a bare-string key (the outcome
  member's NAME instead of the member) and a non-string value red on
  both checkers. These are the defects a writer makes in the dark and
  the runtime only convicts at the FIRST record's route.
* FACE 2 — the RUNTIME (validate()/Chain's declaration door, tested in
  ``tests/test_wf_t20_router_pins.py::test_t20_route_not_total_is_refused_at_declaration``):
  a NON-TOTAL map — every key IS the outcome enum, one member missing —
  is deliberately NOT a checker error: a dict literal's key set is not
  a static totality contract in either checker, and pretending
  otherwise (a complete-mapping sentinel type) would be the compile's
  over-rejection. The totality is the declaration door's refusal:
  ``chain step 'screen': route is not total over ScreenOutcome —
  missing ['flagged']`` — the missing outcome would silently strand a
  record's chain.

Run:  uv run --no-sync python tests/typeprobe/_gate.py
"""

from __future__ import annotations

import enum

from taskq.workflows import Route


class ScreenOutcome(enum.Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


def probe_route_key_is_a_bare_string() -> None:
    """The outcome member's NAME spelled as a bare string — the dict's
    key type is the enum; the runtime would register the route under the
    raw text and strand the first record on RouterNotTotal. The CHECKER
    is the door that catches it at writing time."""
    route = Route(
        {
            "CLEAN": "enrich",  # MUST_ERROR(reportArgumentType, invalid-argument-type): a string key is not the outcome member
            ScreenOutcome.FLAGGED: "review",
        }
    )
    _ = route


def probe_route_value_is_not_a_step_key_type() -> None:
    """The route arm is a step key (a str) or DONE (None) — an int arm
    is the typo the checker names at the call site."""
    route = Route(
        {
            ScreenOutcome.CLEAN: 42,  # MUST_ERROR(reportArgumentType, invalid-argument-type): the arm is a step key or None
            ScreenOutcome.FLAGGED: "review",
        }
    )
    _ = route


def probe_route_non_total_is_the_runtime_face() -> None:
    """THE TWO-FACES HONESTY (no marker — the unmarked-error rule keeps
    this line green): a NON-TOTAL map whose keys are all genuinely the
    outcome enum is WELL-TYPED — neither checker reds it, because a
    dict literal's key set carries no static totality contract. This is
    the deliberate face split: the CHECKER owns the key/value TYPES
    (the probes above); the RUNTIME owns the totality (the declaration
    door's refusal — ``missing ['flagged']`` — pinned in
    ``tests/test_wf_t20_router_pins.py::test_t20_route_not_total_is_refused_at_declaration``)."""
    route = Route(
        {
            ScreenOutcome.CLEAN: "enrich",
        }  # NOT a checker error: well-typed, non-total — validate()'s face
    )
    _ = route
