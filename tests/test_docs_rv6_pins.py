"""ATTACK PINS — the rv6 round's DOCS pack (fix/docs-currency @ 6e2297fe8),
PORTED TO the engine-cures lane and FLIPPED GREEN by its doc cures.

Provenance: the rv6 front's convictions over the docs estate. Three were
LIVE at the pinned head (pinned ``pytest.mark.xfail(strict=True)``
asserting the SAFE behavior — the cure flips them XPASS-strict). THE
FLIP IS DONE: the cures (the api-reference's wiring-verbs table carries
the `route` + `chain_source` rows; the guide's fan-in section NAMES
`FanInBoundExceededError` beside its remedy; the guide's route teaching
states the single-value disposition) removed the markers WITH the cure —
the assertions stand as the permanent green pins. F-DOCS-4's cited text
(the holdmap cure's runbook paragraph) does NOT exist in this tree — it
lives on the unmerged ``feat/taskqflow-holdmap-cures`` lane — so that
pin stands GREEN as the anti-rot guard: the docs-numbers law it asserts
convicts the text the day the lane lands with the mismatch (the rv2-11
posture: a guard that keeps the resolution from rotting, here pre-seeded
for the incoming paragraph). All four pins are STATIC (the estate's own
text, never a container).

The findings:

* F-DOCS-1 — the api-reference's wiring-verbs table
  (``docs/api-reference/workflows.md``, "The wiring verbs") omits
  ``route`` — a top-level export (``taskq.workflows.__all__``), the T27
  typed-route verb whose own section the guide teaches. THE PIN (static):
  every public verb in ``taskq.workflows.__all__`` that is a wiring verb
  (a function homed in the graph-DSL wiring layer) appears as a table row
  (walk ``__all__`` vs the table; ``route`` must be present).
* F-DOCS-2 — ``FanInBoundExceededError`` exists in src
  (``workflows/definitions.py``) but nowhere in ``docs/`` — the fan-in
  bound's section teaches the bound and the child-driven escape but never
  NAMES the error class the operator meets. THE PIN: the guide's fan-in
  bound section names the error class with its remedy.
* F-DOCS-3 — the single-value ``Promise[A|B]`` disposition is unstated:
  ``route()`` is documented only as the per-element (list-source) map
  face; the build refusal for a non-list source ("the route is a MAP
  face") and the single-value alternative (the chain step's in-body
  ``Route``) are nowhere named. THE PIN: the guide's route/conditional
  section names the single-value disposition explicitly.
* F-DOCS-4 — (the holdmap lane's runbook text, NOT at this tree): "served
  by five ``wf_*`` tables" while listing six ``wf_*`` tables + ``jobs``.
  THE PIN (the docs-numbers pattern, a GREEN guard here): every
  count-and-list claim of the form "N ``wf_*`` tables" in the docs
  matches the adjacent table's actual ``wf_*`` row count.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_GUIDE = _REPO / "docs" / "guides" / "workflows.md"
_API_REF = _REPO / "docs" / "api-reference" / "workflows.md"

#: The graph-DSL wiring layer — the modules whose PUBLIC functions are the
#: flow-definition verbs (the api-reference table's own scope: the verbs
#: called inside the ``@app.workflow`` builder). The runtime faces
#: (``run``, ``cancel_workflow_run``, ``retry_workflow_node`` — homed in
#: ``_run`` / ``_runner_exit``) and the emit machinery's helpers
#: (``chain_start`` / ``chain_fork`` — ``chain.py``, called from source
#: bodies and the engine, never wiring a graph node) are not wiring verbs.
_WIRING_MODULES = {"taskq.workflows.api._graph", "taskq.workflows.api._loop"}


# ── F-DOCS-1: the wiring-verbs table walks __all__ ───────────────────────


def test_docs1_every_wiring_verb_in_all_appears_in_the_table() -> None:
    """The table's completeness law: every public name in
    ``taskq.workflows.__all__`` resolving to a FUNCTION homed in the
    wiring layer (``_graph`` / ``_loop``) is a row in the "The wiring
    verbs" table. The walk is mechanical so a NEW verb landing without
    its row reds the same way; the witness asserts the table exists and
    already carries the known verbs (a deleted table reds loud, never
    vacuous)."""
    import taskq.workflows as wf

    verbs = {
        name
        for name in wf.__all__
        if inspect.isfunction(getattr(wf, name)) and getattr(wf, name).__module__ in _WIRING_MODULES
    }
    assert verbs, "the wiring-verb walk found nothing — __all__ rotted"
    text = _API_REF.read_text()
    section = re.search(r"## The wiring verbs\n(.*?)(?=\n## )", text, re.DOTALL)
    assert section is not None, "the wiring-verbs section is missing from the api-reference"
    rows = re.findall(r"^\| `([a-z_]+)\(", section.group(1), re.MULTILINE)
    documented = set(rows)
    # The witness: the table is real and non-empty (the known verbs land).
    assert {"step", "gather", "build"} <= documented, (
        f"the wiring-verbs table rotted (rows found: {sorted(documented)})"
    )
    missing = verbs - documented
    assert not missing, (
        "F-DOCS-1: public wiring verbs missing from the api-reference table: "
        f"{sorted(missing)} — `route` is a top-level export (the T27 typed "
        "route the guide teaches at length); a reader of the reference can "
        "never find it"
    )


# ── F-DOCS-2: the fan-in bound's error class, named with its remedy ──────


def test_docs2_the_fan_in_bounds_error_class_is_named_with_its_remedy() -> None:
    """The class IS the refusal the operator sees (the source-side
    message names it; the run record carries it as ``error_class``) — the
    guide's fan-in bound section must NAME it beside the remedy
    (``child_driven=True`` / partition), so the met-in-the-wild error
    string resolves to its doc. The witness: the bound IS documented
    (a section rename reds loud, never vacuous)."""
    text = _GUIDE.read_text()
    bound_section = re.search(r"## The fan-in bound[^\n]*\n(.*?)(?=\n## )", text, re.DOTALL)
    assert bound_section is not None, (
        "the fan-in bound section is missing — the witness fails loud (the "
        "section exists at the pinned head)"
    )
    section_text = bound_section.group(1)
    assert "child_driven" in section_text, (
        "the fan-in bound section lost its remedy (the child-driven escape) — "
        "the witness fails loud"
    )
    assert "FanInBoundExceededError" in section_text, (
        "F-DOCS-2: the fan-in bound's error CLASS is named nowhere in the "
        "guide (nor anywhere in docs/) — the operator who meets "
        "error_class='FanInBoundExceededError' finds no doc; the section "
        "teaches the bound (≤ 1000) and the child-driven escape but never "
        "names the refusal's own class"
    )


# ── F-DOCS-3: the single-value route disposition ─────────────────────────


def test_docs3_the_single_value_route_disposition_is_named() -> None:
    """``route(source, arms)`` takes a LIST source — a single-value
    ``Promise[A|B]`` is refused at build ("the route is a MAP face").
    The guide's route/conditional teaching must SAY that, and say where
    single-value routing goes (the chain step's in-body ``Route`` — the
    §router the same guide teaches). The witness: the route verb IS
    documented (a section rename reds loud)."""
    text = _GUIDE.read_text()
    assert "route(source, arms)" in text, (
        "the route verb's documentation is missing — the witness fails loud "
        "(the section exists at the pinned head)"
    )
    # The disposition, stated: a passage naming the single-value (non-list)
    # case AND its disposition (the chain/in-body Route, or route()'s own
    # refusal of the shape).
    disposition = re.search(
        r"(single[- ]value|non-list|not a list|list source)[^\n]{0,240}?"
        r"(Route\(|chain|refus|map face)|"
        r"(Route\(|chain step)[^\n]{0,240}?(single[- ]value|non-list|not a list)",
        text,
        re.IGNORECASE,
    )
    assert disposition is not None, (
        "F-DOCS-3: the single-value Promise[A|B] disposition is unstated — "
        "route() is taught only over list sources ('each element runs ITS "
        "type's arm'), and nothing names where routing a SINGLE union value "
        "goes (the chain step's in-body Route) nor route()'s own build "
        "refusal of the non-list source"
    )


# ── F-DOCS-4: the docs-numbers count-vs-list law (the GREEN guard) ───────

#: The count claim's shape: "five `wf_*` tables" — a number word (or digit)
#: immediately scoping the workflow-table family.
_TABLE_COUNT_CLAIM = re.compile(
    r"\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+`wf_\*` tables", re.IGNORECASE
)

_NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}


def test_docs4_a_wf_table_count_claim_matches_its_list() -> None:
    """THE DOCS-NUMBERS LAW at the table count: a sentence claiming "N
    ``wf_*`` tables" must be followed (in its own block, before the next
    heading) by a table listing exactly N ``wf_*`` rows — the convicted
    shape (the holdmap lane's runbook) says "five ``wf_*`` tables" and
    lists SIX (``wf_edge``/``wf_step_ledger``/``wf_signals``/``wf_outbox``
    /``wf_join_fire``/``wf_node_progress``) beside ``jobs``.

    AT THIS HEAD (6e2297fe8): NO such claim exists — the cited paragraph
    lives on the unmerged holdmap-cures lane (the guide's §7 here carries
    no table-count sentence). This pin stands GREEN as the anti-rot
    guard: the day the lane's paragraph lands with the mismatch, the pin
    reds. Zero matches is a lawful green (the law convicts claims, not
    absence); the §7 witness keeps the read honest."""
    assert _GUIDE.is_file(), "the workflows guide is missing"
    offenders: list[str] = []
    for doc in sorted((_REPO / "docs").rglob("*.md")):
        text = doc.read_text()
        for match in _TABLE_COUNT_CLAIM.finditer(text):
            claim_word = match.group(1).lower()
            claimed = int(claim_word) if claim_word.isdigit() else _NUMBER_WORDS[claim_word]
            # The claim's own block: from the match to the next heading.
            block = text[match.start() :]
            next_heading = re.search(r"\n#{1,3} ", block)
            if next_heading:
                block = block[: next_heading.start()]
            listed = set(re.findall(r"^\| `(wf_[a-z_]+)`", block, re.MULTILINE))
            if listed and len(listed) != claimed:
                offenders.append(
                    f"{doc.relative_to(_REPO)}: claims {claimed} `wf_*` tables "
                    f"but lists {len(listed)} ({sorted(listed)})"
                )
    assert not offenders, "F-DOCS-4: a table-count claim disagrees with its list:\n" + "\n".join(
        offenders
    )
