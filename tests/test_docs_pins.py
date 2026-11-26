"""The docs-currency pins — the docs estate's currency guards.

Three pins held the SAFE behavior at their pinning (xfail-strict) and
the doc fixes flipped them: the api-reference's wiring-verbs table
carries the `route` + `chain_source` rows; the guide's fan-in section
NAMES `FanInBoundExceededError` beside its remedy; the guide's route
teaching states the single-value disposition — the assertions stand as
the permanent green pins. The cited text
(the holdmap fix's runbook paragraph) does not exist in this tree — it
lives on the unmerged ``feat/taskqflow-holdmap-fixes`` lane — so that
pin stands GREEN as the anti-rot guard: the docs-numbers law it asserts
rejects the text the day the lane lands with the mismatch (the 11
posture: a guard that keeps the resolution from rotting, here pre-seeded
for the incoming paragraph). All four pins are STATIC (the estate's own
text, never a container).

The findings:

* the api-reference's wiring-verbs table
  (``docs/api-reference/workflows.md``, "The wiring verbs") omits
  ``route`` — a top-level export (``taskq.workflows.__all__``), the typed route
  typed-route verb whose own section the guide teaches. the PIN (static):
  every public verb in ``taskq.workflows.__all__`` that is a wiring verb
  (a function homed in the graph-DSL wiring layer) appears as a table row
  (walk ``__all__`` vs the table; ``route`` must be present).
* ``FanInBoundExceededError`` exists in src
  (``workflows/definitions.py``) but nowhere in ``docs/`` — the fan-in
  bound's section teaches the bound and the child-driven escape but never
  NAMES the error class the operator meets. the PIN: the guide's fan-in
  bound section names the error class with its remedy.
* the single-value ``Promise[A|B]`` disposition is unstated:
  ``route()`` is documented only as the per-element (list-source) map
  face; the build refusal for a non-list source ("the route is a MAP
  face") and the single-value alternative (the chain step's in-body
  ``Route``) are nowhere named. the PIN: the guide's route/conditional
  section names the single-value disposition explicitly.
* (the holdmap lane's runbook text, not at this tree): "served
  by five ``wf_*`` tables" while listing six ``wf_*`` tables + ``jobs``.
  Every
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

#: The graph-dsl wiring layer — the modules whose public functions are the
#: flow-definition verbs (the api-reference table's own scope: the verbs
#: called inside the ``@app.workflow`` builder). The runtime faces
#: (``run``, ``cancel_workflow_run``, ``retry_workflow_node`` — homed in
#: ``_run`` / ``_runner_exit``) and the emit machinery's helpers
#: (``chain_start`` / ``chain_fork`` — ``chain.py``, called from source
#: bodies and the engine, never wiring a graph node) are not wiring verbs.
_WIRING_MODULES = {"taskq.workflows.api._graph", "taskq.workflows.api._loop"}


# ── the wiring-verbs table walks __all__ ───────────────────────


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
        "Public wiring verbs missing from the api-reference table: "
        f"{sorted(missing)} — `route` is a top-level export (the typed route typed "
        "route the guide teaches at length); a reader of the reference can "
        "never find it"
    )


# ── the fan-in bound's error class, named with its remedy ──────


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
        "The fan-in bound's error CLASS is named nowhere in the "
        "guide (nor anywhere in docs/) — the operator who meets "
        "error_class='FanInBoundExceededError' finds no doc; the section "
        "teaches the bound (≤ 1000) and the child-driven escape but never "
        "names the refusal's own class"
    )


# ── the single-value route disposition ─────────────────────────


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
    # case and its disposition (the chain/in-body Route, or route()'s own
    # refusal of the shape).
    disposition = re.search(
        r"(single[- ]value|non-list|not a list|list source)[^\n]{0,240}?"
        r"(Route\(|chain|refus|map face)|"
        r"(Route\(|chain step)[^\n]{0,240}?(single[- ]value|non-list|not a list)",
        text,
        re.IGNORECASE,
    )
    assert disposition is not None, (
        "The single-value Promise[A|B] disposition is unstated — "
        "route() is taught only over list sources ('each element runs ITS "
        "type's arm'), and nothing names where routing a SINGLE union value "
        "goes (the chain step's in-body Route) nor route()'s own build "
        "refusal of the non-list source"
    )


# ── the docs-numbers count-vs-list law (the green guard) ───────

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
    """The docs-numbers law at the table count: a sentence claiming "N
    ``wf_*`` tables" must be followed (in its own block, before the next
    heading) by a table listing exactly N ``wf_*`` rows — the rejected
    shape (the holdmap lane's runbook) says "five ``wf_*`` tables" and
    lists SIX (``wf_edge``/``wf_step_ledger``/``wf_signals``/``wf_outbox``
    /``wf_join_fire``/``wf_node_progress``) beside ``jobs``.

    AT THIS HEAD (6e2297fe8): no such claim exists — the cited paragraph
    lives on the unmerged holdmap-fixes lane (the guide's §7 here carries
    no table-count sentence). This pin stands GREEN as the anti-rot
    guard: the day the guide's paragraph lands with the mismatch, the pin
    reds. Zero matches is a lawful green (the law rejects claims, not
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
    assert not offenders, "A table-count claim disagrees with its list:\n" + "\n".join(offenders)


def test_docs5_the_upgrading_guide_pins_the_folded_backpressure_train() -> None:
    """The contract §10.8's named PR-3 edit, pinned: the enumeration is
    exactly `01.00.38_01`, `01.00.41_01`, `01.00.41_02`, `01.00.42_01` (the
    withdrawn `41_03`/`41_04`/`41_05`/`41_06` identities appear nowhere in
    the instructions); the exact 'two existing 01.00.41 files' wording
    stands; the section heading and the one-file instruction are exact."""
    import re
    from pathlib import Path

    guide = (Path(__file__).parent.parent / "docs" / "guides" / "upgrading.md").read_text()

    # The section heading (the contract's exact text).
    heading = "## Slot-free emit backpressure, durable intent, and per-claim ledger (01.00.42_01)"
    assert heading in guide, "the folded 42_01 section heading is missing or altered"
    section = guide.split(heading, 1)[1].split("\n## ", 1)[0]

    # The one-file instruction, exact.
    instruction = (
        "Apply the one PRE file 01.00.42_01_pre_wf_step_ledger_claim_epoch.sql "
        "after the 01.00.38 + existing 01.00.41 files and before corrected "
        "bounded-emit/executor binaries; it installs the derived-capacity "
        "intent/rotation records, exact-park guard/indexes, and claim-epoch "
        "arbiter in one migration transaction."
    )
    assert " ".join(instruction.split()) in " ".join(section.split()), (
        "the 42_01 apply instruction must be exact"
    )

    # The withdrawn identities are never instructed.
    for withdrawn in ("01.00.41_03", "01.00.41_04", "01.00.41_05", "01.00.41_06"):
        assert withdrawn not in section, withdrawn

    # The base section's parenthetical carries the exact wording.
    assert (
        "Applying the two existing 01.00.41 files (`01.00.41_01` and\n"
        "`01.00.41_02`) without `01.00.38_01` fails at" in guide
    ) or (
        "Applying the two existing 01.00.41 files (`01.00.41_01` and `01.00.41_02`) "
        "without `01.00.38_01` fails at" in " ".join(guide.split())
    ), "the parenthetical's exact enumeration wording moved or changed"

    # The enumerated train in the backpressure section: exactly the four
    # keys (parsed from the section's own file names).
    files = re.findall(r"01\.00\.\d+_\d+[a-z_]*\.sql", section)
    assert files == ["01.00.42_01_pre_wf_step_ledger_claim_epoch.sql"], files


def test_docs7_the_upgrading_guide_states_the_10_7_operational_facts() -> None:
    """RULING_718_ROUND3's F9 narrow pins: `upgrading.md`'s §10.7
    operational-facts paragraph states each binding fact and stays
    current — the fixed 200/100 ceilings, the scoped sub-second wake
    target, `parked <= S`, `Theta(live + parked)`, and the non-drainage
    refusal's durable-only export (the metric counter named, never an
    importable class)."""
    text = (_REPO / "docs" / "guides" / "upgrading.md").read_text(encoding="utf-8")
    section = re.search(
        r"\*\*Operational facts \(§10\.7\):\*\*(.*?)(?=\n## |\nThe folded file)",
        text,
        re.DOTALL,
    )
    assert section is not None, "the §10.7 operational-facts paragraph is missing"
    body = section.group(1)
    assert "**200** winning" in body, body
    assert "**100**" in body, body
    assert "**< 1 s**" in body and "competent fleet" in body, body
    assert "`parked <= S`" in body, body
    assert "`Theta(parked) <= Theta(S)`" in body, body
    assert "`Theta(live + parked)`" in body, body
    assert "`taskq.workflows.wf_not_draining`" in body, body
    assert "never as an\nimportable class" in body or "never as an importable class" in body, body
    assert "maintenance interval" in body, body


def test_docs6_author_facing_docs_never_publish_internal_capacity_vocabulary() -> None:
    """§10.1's API-surface leak pin (D9): the internal capacity vocabulary —
    the derived-charge token, the intent generations, the derived
    minimum-waiter width, the internal table names, the rotation cursor,
    and the validated scalar renderers — is not author-facing API and
    must not be published in the author-facing docs (the emit
    backpressure guide and the API reference). Architecture/upgrade
    material is the documented home for the internal names and is out of
    this pin's scope; a leak reds with the offending file and token."""
    import re

    banned = (
        r"\bcharge\b",
        r"\bmin_waiting_width\b",
        r"\bwake_generation\b",
        r"\bhandled_generation\b",
        r"\bwf_emit_capacity\b",
        r"\bwf_emit_intent_rotation\b",
        r"\bvalidated_park_width\b",
        r"\bvalidated_reservation_width\b",
        r"\bafter_flow_id\b",
    )
    for doc in (_GUIDE, _API_REF):
        text = doc.read_text()
        for token in banned:
            hits = [text[: match.start()].count("\n") + 1 for match in re.finditer(token, text)]
            assert not hits, (
                f"{doc.relative_to(_REPO)} publishes the internal capacity "
                f"vocabulary {token!r} at line(s) {hits[:5]} — internal "
                "table names, charge, generations, and min_waiting_width are "
                "not author-facing API (§10.1); teach the author-facing "
                "behaviour instead."
            )


# ── the deleted backpressure mechanism's vocabulary ────────────

#: #718's shipped mechanism raises ``EmitBackpressure`` IMMEDIATELY on a
#: no-fit page — there is no bounded poll at the admission fence — and
#: the runner parks the source (the ledger records ``awaited``, never a
#: failure; the park is attempt-free). The phrases below are that
#: DELETED pre-#718 mechanism's own vocabulary: a bounded poll at the
#: fence, the failure-ladder-owned stall, the pager's term, the waiting
#: child. Their reappearance on a current-facing docs page — or on the
#: product tree's own authoring surfaces (``src/taskq/`` docstrings,
#: string literals, and comments: a shipped docstring RIDES THE API,
#: so it is worse-placed than a docs page, not better) — teaches a
#: behaviour the product no longer has (the final whole-diff review's
#: KF-1 finding: four pages still taught it, and a fifth sentence of
#: the same class survived in the ``max_in_flight`` docstring because
#: the pin then scanned ``docs/`` only). Each phrase is
#: multi-word and backpressure-fence-specific — generic bounded
#: polling on an unrelated surface (a test seam, an SSE stream) is not
#: this pin's subject and does not match.
#:
#: The last two members are the fifth surface's own spans, and they
#: are here on purpose: the docstring sentence read "the stall rides
#: the CERTIFIED ladder" — the interpolating adjective defeated the
#: exact-substring match "stall rides the ladder", which is exactly
#: how the sentence survived both the KF-1 sweep and the six-phrase
#: list. The spans are added literally (no gap-tolerant regex: a
#: "stall …ladder" pattern false-positives every file that mentions
#: both words and would turn the allowlist into a judgment call); the
#: pre-fix-archive red pins that they fire on the class, and the
#: green tree pins that they fire on nothing else.
#:
#: The last three members are the same class caught one surface later
#: (the fresh-DeepSeek round-25 finding): the test-family table's own
#: backpressure row still said "the pager blocks at the declared
#: bound … the throttle releases under a draining worker" — a WAITING
#: description of a mechanism that refuses — and the row survived
#: every sweep because none of its phrases was listed. "the pager
#: blocks" (the general waiting claim about the pager), "blocks at
#: the declared bound" (the exact surviving clause) and "the throttle
#: releases" (the sibling waiting claim) are here for the same reason
#: the ladder spans are: each is a literal substring of a sentence
#: that has already survived a review, and the list is the record of
#: the vocabulary the sweeps have had to convict so far.
_BACKPRESSURE_LEGACY_PHRASES = (
    "bounded poll",
    "ladder-owned stall",
    "stalls ride the ladder",
    "stall rides the ladder",
    "the pager's term",
    "waits at the fence",
    "stall rides the certified ladder",
    "laziness is the pause",
    "the pager blocks",
    "blocks at the declared bound",
    "the throttle releases",
)

#: Every scanned markdown surface is current-facing (the docs build
#: publishes all of ``docs/``; the front page and the examples' pages
#: ship in the repository root and with the sdist's examples), so the
#: allowlist is EMPTY by construction. A genuinely historical page —
#: an archived design note kept for provenance — is exempted BY PATH,
#: with its reason recorded beside it, never by phrase: an exemption
#: must name the file, and a file that returns to current-facing
#: teaching must lose the entry.
_BACKPRESSURE_LEGACY_ALLOWLIST: dict[str, str] = {}

#: The product tree's allowlist, same discipline: keyed BY PATH with the
#: reason recorded beside the entry, never by phrase, and empty unless an
#: unrelated surface genuinely forces one. The scan reads the WHOLE
#: module file (docstrings, string literals, and comments — the
#: codebase's authoring surfaces), whitespace-collapsed, exactly like
#: the docs pass.
_BACKPRESSURE_LEGACY_SRC_ALLOWLIST: dict[str, str] = {
    "src/taskq/web/admin/jobs.py": (
        "the admin SSE progress stream's own checkout comment says "
        "'bounded polls' for the stream's BoundedPool checkouts — "
        "generic bounded polling on an unrelated surface (the control's "
        "declared out-of-scope class), not the emit admission fence"
    ),
}


def test_docs8_no_current_facing_doc_teaches_the_deleted_backpressure_behaviour() -> None:
    """KF-1's maintained negative control: the deleted pre-#718
    backpressure mechanism's vocabulary must not reappear on a
    current-facing surface. The scanned corpus, exactly:

    * every ``*.md`` under ``docs/`` (the docs build publishes all of
      them; none is an archive);
    * the repository's front page (``README.md``) and every ``*.md``
      the shipped ``examples/`` carry — the round-25 review executed
      the bypass: a banned phrase pasted into either left this pin
      green while it scanned ``docs/`` only;
    * every ``*.py`` under ``src/taskq/`` — docstrings, string
      literals, and comments, the product tree's own authoring
      surfaces (the ``max_in_flight`` docstring is exactly the surface
      an author reads when choosing a bound, so the product scan is
      not a bonus, it is the class's likeliest home).

    Deliberately NOT scanned, by path, with the reason: ``CHANGELOG.md``
    (append-only release history — a past-tense entry may name the
    pre-#718 behaviour truthfully), the ``perf-evidence-*.md`` reports
    (frozen per-run evidence — provenance artifacts, not behaviour
    teaching), the policy pages (``CONTRIBUTING.md``,
    ``CODE_OF_CONDUCT.md``, ``SECURITY.md``, ``SUPPORT.md``), and
    everything outside those roots (build outputs, site-packages, the
    preserved ``rescue/`` snapshot). The pin's claim is exactly this
    corpus: a surface that becomes current-facing teaching joins the
    scan, or its path is exempted below with a written reason.

    The declared residual: this is a LITERAL scanner. It convicts the
    exact vocabulary the review sweeps have already met (the phrase
    list is that record) and cannot catch a never-seen paraphrase of
    the deleted mechanism — "no current-facing surface teaches the
    deleted behaviour" is enforced here as "no scanned surface uses
    the convicted phrases". The residual is discharged by the review
    discipline, not by this pin: each review round re-reads the
    current-facing surfaces against the shipped mechanism (the KF-1
    sweep), and every phrase a sweep convicts joins the list.

    The shipped law is the guide's "Slot-free emit backpressure"
    section — saturation raises ``EmitBackpressure`` immediately and
    parks the source; nothing polls at the fence and nothing rides
    the failure ladder.

    The comparison collapses whitespace across the whole file first, so
    a phrase cannot hide by wrapping across lines — for ``src/`` the
    whole module file is the unit, exactly like a docs page. Two
    witnesses keep the read honest: the guide's shipped-mechanism
    sentence (the docs corpus' anchor) and the ``EmitBackpressure``
    docstring's own shipped-law sentence (the src corpus' anchor) — if
    either is deleted or renamed, this pin reds loud instead of passing
    vacuously on an empty corpus."""
    guide_text = _GUIDE.read_text(encoding="utf-8")
    assert "saturation is a park, not a failure" in " ".join(guide_text.split()), (
        "the guide's shipped-mechanism sentence is gone — the witness "
        "fails loud (the pin's own subject must exist to be enforceable)"
    )
    emit_text = " ".join(
        (_REPO / "src" / "taskq" / "workflows" / "_emit.py").read_text(encoding="utf-8").split()
    )
    assert "deliberately outside ``Snooze``/``RetryAfter`` and the failure ladder" in emit_text, (
        "the EmitBackpressure docstring's shipped-law sentence is gone — the "
        "src witness fails loud (the pin's product anchor must exist to be "
        "enforceable)"
    )
    offenders: list[str] = []
    markdown = sorted((_REPO / "docs").rglob("*.md"))
    markdown.append(_REPO / "README.md")
    markdown.extend(sorted((_REPO / "examples").rglob("*.md")))
    for doc in markdown:
        key = doc.relative_to(_REPO).as_posix()
        if key in _BACKPRESSURE_LEGACY_ALLOWLIST:
            continue
        text = " ".join(doc.read_text(encoding="utf-8").split())
        for phrase in _BACKPRESSURE_LEGACY_PHRASES:
            if phrase in text:
                offenders.append(f"{key}: {phrase!r}")
    for module in sorted((_REPO / "src" / "taskq").rglob("*.py")):
        key = module.relative_to(_REPO).as_posix()
        if key in _BACKPRESSURE_LEGACY_SRC_ALLOWLIST:
            continue
        text = " ".join(module.read_text(encoding="utf-8").split())
        for phrase in _BACKPRESSURE_LEGACY_PHRASES:
            if phrase in text:
                offenders.append(f"{key}: {phrase!r}")
    assert not offenders, (
        "A current-facing doc or product surface teaches the DELETED "
        "pre-#718 backpressure behaviour (a bounded poll at the admission "
        "fence / a failure-ladder-owned stall). The scanned corpus is "
        "docs/**/*.md, README.md, examples/**/*.md and src/taskq/**/*.py "
        "(the docstring records what is deliberately out and why). The "
        "shipped mechanism raises EmitBackpressure immediately on a "
        "no-fit page and parks the source (docs/guides/workflows.md, "
        "'Slot-free emit backpressure: saturation is a park, not a "
        "failure'); rephrase the surface to the shipped mechanism:\n" + "\n".join(offenders)
    )


def test_docs_join_expiry_replay_is_scoped_to_execution() -> None:
    """The child-join example must not teach the old attempt-scoped face:
    a refunded-attempt re-claim is a new execution and replays ``Expired``.
    The replay slot is one per node row and attempt, not per child, and the
    guide must say so rather than promise a re-hold that the two-expired-
    children shape cannot honour."""
    text = " ".join(_GUIDE.read_text(encoding="utf-8").split()).lower()
    assert "handle.wait()" in text, "the child-join example is missing"
    assert "abandoned hold per execution" in text
    assert "abandoned hold per attempt" not in text
    assert "one replay slot per node row and attempt" in text
    assert "two expired children" in text


def test_docs9_the_upgrading_guide_names_the_replay_marker_key_families() -> None:
    """The wait/timer/join replay cursors moved from attempt-keyed metadata
    (``hold_cursor_*``, ``child_join_face_*``) to claim-scoped ones
    (``hold_replay_signal_*``, ``hold_replay_timer_*``,
    ``child_join_replay_*``) with no migration. An operator rolling binaries
    across this change must be able to find the old keys on a row, read the
    new ones, and know which family the running worker believes — the
    practice this guide already follows for ``spawn_cursor_`` →
    ``spawn_slot_``, including a ``jsonb_object_keys`` detection query."""
    text = (_REPO / "docs" / "guides" / "upgrading.md").read_text(encoding="utf-8")
    body = " ".join(text.split())

    for family in (
        "hold_cursor_",
        "child_join_face_",
        "hold_replay_signal_",
        "hold_replay_timer_",
        "child_join_replay_",
    ):
        assert family in body, (
            f"docs/guides/upgrading.md never names the {family!r} replay cursor key "
            "family: an operator cannot find the old cursors on every wait row, nor "
            "tell which family the running worker believes"
        )
    assert "inert and unmigrated" in body
    assert "rollback caveat" in body.lower()
    assert "jsonb_object_keys" in body
    assert "starts_with(k, 'hold_cursor_')" in body
    assert "starts_with(k, 'child_join_face_')" in body
