# Why: no SQL is constructed here; the scanner reads repository text and ASTs only, and every path comes from the tree.
"""The stranded-reason VOCABULARY INVARIANT (the round-3 ruling's seventh
layer / N7-1): ONE tree-wide, semantic test owns completeness for the
stranded-reason prose/value class — never another hand-maintained list
of sites.

The class: closed reason-set enumerations of the stranded detector, the
universal never-dispatchable gloss (the exact variant spellings are
_UNIVERSAL_VARIANTS, matched case-folded against every local unit),
whole-detector parity/mirror claims, and the typed reason-set
declarations. Three consecutive review rounds found the same defect
class on further shipped surfaces; this invariant replaces hand
enumeration with a maintained scanner.

THE SCAN — every file under src/, docs/ and tests/ carrying a .py, .md,
.yml, .yaml or .html suffix is parsed into SEMANTIC LOCAL UNITS, every
unit is matched against the four rules, and a match outside the
span-digest allowlist is red. The units, per surface (each surface owns
its text with its real grammar's parser):

* .md — markdown-it's (a CommonMark block parser's) token/line map:
  every paragraph, ATX and setext heading, thematic break, list item at
  any depth, blockquote paragraph (lazy continuation included), fenced
  or indented code block, raw HTML block and table row, each at its own
  true line span; PLUS the MkDocs CONTAINER bodies the docs build
  enables (the `!!!`/`???`/`===` admonition, details and tab bodies —
  the renderer's own block-processor regexes and detab law,
  `_container_marker` / `_container_body_end`), the body dedented and
  re-parsed as markdown at a line offset, blockquote-prefixed
  containers included (`> !!! note` is matched after the renderer's
  blockquote clean law and gets the same body paragraph units as the
  unquoted form); PLUS each fenced/indented code block's comment
  statements — full-line (`--`/`//`/`#`/`%`/`;`) and trailing-after-
  code line comments (the marker needs NO preceding whitespace:
  `SELECT 1;-- x` is a comment statement too), and `/* */` / `<!-- -->` block comments as whole
  statements; PLUS the per-line FLOOR: every non-blank line no unit
  owns (a link-reference definition the parser consumes without a
  token, a `>`-only separator, any construct the model does not know)
  is its own unit, never folded. The markdown grid is the renderer's
  own (`_renderer_lines`): newlines folded and every tab expanded to
  the next 4-column tab stop — the renderer's NormalizeWhitespace — so
  the detab slice removes the columns it claims.
* .py — AST docstrings, string constants (a JoinedStr f-string's
  literal parts joined into ONE unit), each comment line its own unit
  (tokenize follows every comment token with the next non-COMMENT
  token — an NL after a full-line comment and after a trailing comment
  inside an open bracket, where the logical line continues, a NEWLINE
  after a trailing comment that ends the logical line — and the unit
  block flushes on that next token, so two adjacent comment lines are
  sibling units, never folded), and
  the rule-4 typed collection / `Literal[...]` source segments. For the
  coverage law a Python file's text carriers are its tokenize-level
  STRING/COMMENT tokens.
* .yml/.yaml — parsed scalar values at the composer's exact node marks
  (keys and values both), each comment line (full-line and trailing),
  and the per-line floor.
* .html — each element's OWN run (a leaf element is one unit — a
  `<tr>` row, a `<span>` badge — a wrapped element stays ONE unit; a
  container contributes start-tag + direct-text + end-tag runs while
  each CHILD element is its own unit), every `<!-- -->` and `{# #}`
  comment statement (a multi-line comment stays the one statement it
  is), and the per-line floor for the text no element or comment owns.

THE FOUR RULES, over every unit:

1. CLOSED ENUMERATION — the unit defines the stranded reason family
   with only the old dispatch conditions (the two-label set, the prose
   two-member form, or the single-member definition); a corrected unit
   carries `workflow_root_missing` or is expressly scoped.
2. UNIVERSAL GLOSS — a never-dispatchable claim about the
   detector/gauge population that its own occurrence does not waive
   (the scoping bounds below).
3. WHOLE-DETECTOR PARITY — a parity/mirror claim about the whole
   detector that the unit does not narrow to the two
   dispatch-invisible legs.
4. STRUCTURAL CLOSED ENUMERATION — a `Literal[...]`/tuple/list/set
   declaration carrying both old reason labels and lacking
   `workflow_root_missing`.

THE SCOPING PREDICATE'S BOUNDS. A scope MARKER is word-bounded and
names the legs themselves: `dispatch-invisible`, `dispatch invisible`,
`may dispatch`, `may be dispatched` (`_SCOPE_MARKER_RE`). A marker is
genuine unless a NEGATOR (`_NEGATOR_RE`) sits in the marker's own
sentence before it — the same SENTENCE, not a character window; the
sentence grid splits after `.`, `!` and `?` only, so a scope binds
across colons, semicolons and dashes — or a positive COUNTER-CLAIM
(`_SCOPE_COUNTER_CLAIM_RE`) shares the marker's sentence, unless that
sentence is the third leg's own definition (it names the third leg,
whose own meaning is the row that CAN dispatch when served). Rule 2's
occurrence is waived when its own sentence carries a genuine marker,
or the occurrence is one reason's own true meaning: its subject is one
reason's own rows (a label and its noun immediately before it, and no
population subject anywhere in the sentence before it), it is bound to
one reason's label with no population subject between the label and
the occurrence, and a population subject anywhere earlier in the
sentence defeats it too unless the occurrence sits inside the last
label's own unclosed parenthetical (`_label_bound` — the shipped alert
descriptions' elliptical form; the exception is paren-bound and does
not verify that the label is the claim's subject — residual 4), or its
own sentence is a foreign
restricted subject with no detector mention (`_foreign_subject`). Rules 1, 3 and
4 are waived by a genuine marker anywhere in the unit (their claims
bind to the unit's own scope statement).

THE THREE STRUCTURAL LAWS, asserted over EVERY scanned file. Together
they put every non-blank line of a text-carrier surface inside some
unit — for .py the law is TOKEN ownership, not line coverage (every
tokenize-level STRING/COMMENT token's span is owned by a unit; a bare
code line owns no text of its own, but some code lines still fall
INSIDE a rule-4 collection / `Literal[...]` unit's span — an
`__all__ = [` opening line, an annotated list assignment) — keep unit
spans laminar, and keep unit text truthful — so a construct no round
thought of cannot live OUTSIDE the units (what a unit's own interior
waives is the scoping predicate's declared business: the residuals
below):

1. LINE COVERAGE — every non-blank line of a text-carrier file belongs
   to at least one unit; for .py, every tokenize-level STRING/COMMENT
   token's span is owned by a unit. The per-line floors make the
   text-carrier form true by construction; the law makes losing a
   floor red.
2. LAMINAR SPANS — every PAIR of a file's units is disjoint, one
   contains the other, or they touch at a shared boundary line,
   decided by the open-interval stack over units sorted by (start,
   longest first) — O(n log n) per file; the corpus test asserts
   agreement with the brute-force all-pairs form on randomized inputs,
   nested-crossing shapes included.
3. NO TRUNCATION — a unit's text is consistent with its declared line
   span: whole-line units carry exactly the file's own grid lines over
   the span minus the container dedent; statement units are exact
   substrings of their span; Python string units carry parsed values,
   so their law is the span's existence plus the ownership law.

THE ALLOWLIST — `path + normalized-span digest + one-line reason`: an
unlisted match is red; a listed entry whose path or span vanishes is
red (deleting text cannot silently erase an exception); a listed
entry's live unit must still FIRE at least one rule — a live-but-not-
firing entry is a STALE SUPPRESSION, hiding nothing today while
silently suppressing a future widened rule on the same span; a
wildcard entry's file must still carry the class's vocabulary. Adding
an entry carries the same independent review as prose policy.

FAIL-CLOSED — an unreadable file, a .py that fails ast.parse,
unreadable YAML, an unparseable template, a fence whose content-line
count disagrees with its span, and a Markdown block token the unit
model does not know are all RED with the path named, never a silent
skip.

DECLARED RESIDUALS — what a gloss could still hide, and why each is
accepted:

1. A gloss inside a fence's NON-COMMENT content (a string literal, a
   code line) lives inside the fence's own coarse unit, whose single
   sentence a sibling scoped comment's marker can waive. The comment
   statements have their own units; the non-comment content does not.
   Accepted: shipped fences' executable lines are code, not prose, and
   every line a scanned comment marker touches — full-line or
   trailing, with or without preceding whitespace — is its own unit,
   and the `/* */` / `<!-- -->` block comments are whole-statement
   units (the marker scan's own law, `_code_comment_units`).
2. A gloss split across `+`-concatenated Python string constants hides
   (each constant is its own unit and the variant spans the
   concatenation); implicit concatenation folds in the AST and does
   not hide. Accepted: shipped source is reviewed in diff, and the
   corpus values are assembled at runtime from independently
   non-triggering fragments while the module's regex/name constants
   are fragment-joined in source — and the scan enforces the
   discipline on itself, because this file is inside the scanned scope
   (a stored triggering combination would red the tree-wide
   invariant).
3. A universal gloss in the same SENTENCE as a genuine scope marker —
   set off only by a colon, semicolon, dash or parenthesis — is waived
   (the sentence is the scoping predicate's granularity; the shipped
   scope sentences bind their own glosses across them). A MARKER in a
   separate sentence never waives the gloss — the waiver binds to the
   gloss's own sentence (the round-10 F10-4 negative); the gloss's own
   sentence can still waive it by subject or label (residual 4).
4. The subject/label waivers are lexical. The round-11 F11-2 position
   gap is closed: a population subject anywhere in the sentence before
   the occurrence defeats them, so "all stranded entries like <label>
   rows <universal>" reds. The one exception — the occurrence inside
   the last label's own unclosed parenthetical (`_label_bound`, the
   shipped alert descriptions' elliptical form) — is PAREN-BOUND, not
   subject-verified (round-12 D12-3): the code checks only that the
   `(` count after the last label EXCEEDS the `)` count (a close-heavy
   unbalanced tail is not the exception — round-13 N13-6) and never
   that the label is the claim's subject, so a sentence whose subject is the
   population and whose label is only mentioned — "All stranded
   entries, unlike <label> (<universal>." or "All stranded entries
   share the <label> defect (so they are <universal>." — hides the
   same way; the three shapes are pinned as the accepted controls in
   `test_scoping_requires_a_genuine_scope`. Accepted: the exception's
   only shipped users are the two alert descriptions' parentheticals,
   where the parenthetical IS each label's own meaning (deleting the
   exception false-reds exactly those two spans — executed), and a
   lexical check that the label is the claim's subject would hand-tune
   to the shipped wording; a population subject INSIDE the
   parenthetical still defeats the waiver — and that red is the
   exception's WIDENING tripwire, pinned as a control (round-13
   N13-5): a widening mutant that drops the between-text check passes
   every other corpus pin and the tree scan, so the inside-paren
   control is the only thing that reds it. What remains
   beyond that is the SPELLING gap: a population subject spelled
   outside `_POPULATION_SUBJECT_RE`'s forms ("every stranded entry …")
   would still waive. Accepted: the forms enumerate the shipped prose's own
   population subjects; a new spelling is a re-review, not a silence.
5. A quoted container's prefix is the renderer's clean law
   (` {0,3}> ?` per level): a tab inside a blockquote prefix is not
   stripped, and such a line degrades to the floor — still covered,
   never hidden.
6. Sentence splitting is mechanical: an abbreviation ("e.g.") inside a
   scope sentence splits it, which can only SEPARATE a claim from its
   scope — more reds, never fewer.
7. The foreign-subject refusal is lexical too: the pronoun set
   (`_PRONOUN_SUBJECT_RE`) is the anaphoric paradigm — third-person
   personal/possessive/reflexive (they/them/their/theirs/themselves,
   it/its/itself), first-person plural (we/us/our/ours/ourselves) and
   demonstratives (these/those/all/such); round-13 N13-1 widened it
   from the seven-word set after the possessive/reflexive/plural
   forms were demonstrated hiding ("Their rows <universal>.", "The
   gauge itself <universal>.", "We <universal>."). A pronoun spelled
   outside the set would still waive. Accepted: the set enumerates
   the shipped prose's own anaphoric subjects (the tree-wide scan
   stays green over it — no shipped sentence needed a declared
   exception); a new pronoun spelling is a re-review, not a silence.

This test is the SOLE completeness authority for this class, over the
scan scope stated above (a surface outside those roots and suffixes is
out of scope by declaration). Per-site red-first evidence (every
corrected site's own old wording reverts and reds this scanner) lives
in the correction report; a new occurrence anywhere in that scanned
scope reds THIS test without waiting for another review to find it —
unless it hides inside a declared residual above, which is exactly
what those seven paragraphs enumerate."""

from __future__ import annotations

import ast
import bisect
import hashlib
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Final

import pytest
from markdown_it import MarkdownIt
from markdown_it.token import Token

REPO_ROOT = Path(__file__).parent.parent

# ── the scan scope ──────────────────────────────────────────────────────
SCAN_ROOTS: tuple[str, ...] = ("src", "docs", "tests")
SCAN_SUFFIXES = {".py", ".md", ".yml", ".yaml", ".html"}

#: The span-digest allowlist: (normalized span digest, path, one-line
#: semantic reason). An unlisted match is RED; a listed entry whose path
#: or span vanishes is RED (deleting/moving text cannot silently erase
#: an exception; adding one carries the same independent review as prose
#: policy).
ALLOWLIST: tuple[tuple[str, str, str], ...] = (
    # ── the whole-file entry (the ruling's own whole-file class: the
    # frozen vendor oracle, and ONLY it — a generic file-wide exclusion
    # for any other file is forbidden) ──────────────────────────────────
    (
        "*",
        "tests/_audit_vendor/_old_leader_sweeps.py",
        "the frozen differential oracle — never current operator text; no edit authorized; its required existence is asserted",
    ),
    # ── the span-digest entries (generated from the triaged scan; the
    # anti-rot law: a wording touch re-derives the digest and re-reviews
    # the span; a vanished span is red) ─────────────────────────────────
    (
        "9e973b0844fcac94f98cf024cde18418cedc2d985a3d129671e817af5d92bba9",
        "src/taskq/actor_config_ops.py",
        "actor-deregistration/missing-row prose about its own subject — not the gauge's reason set",
    ),
    (
        "1344835a0369814f2ccc987d2e649d91466df4896750b4acce5ceca38dbf4bd1",
        "src/taskq/cli.py",
        "the doctor's roll-up SQL's own two dispatch-invisible count columns (the f-string's rendered text is one JoinedStr unit — this entry replaces the old per-chunk digests; the unit's span is the WHOLE f-string literal, wider than any one interpolation chunk it replaced)",
    ),
    (
        "c842b501aea0bfaf6804636b11729bb9cd432f4344c82095d225fa6d63d4df1f",
        "src/taskq/web/templates/_partials/queue_table.html",
        "the queue badge tooltip's own sanctioned two-leg text (layer-6 table B: the tooltip exactly describes the two legs its SQL counts), as its own `<span>` element unit; the old markdown-floor span of the same text is pruned — the template floor is no longer a markdown pass",
    ),
    (
        "cb4f430adf27cd1a6369112810469205a9779f187ecd2752941998609ffe1eae",
        "docs/guides/observability.md",
        "the alert inventory list item (the shipped rules' names), not a reason enumeration",
    ),
    (
        "f84ee18a7c76d0f8af384f67b08bf3c6c5e20dd9a9be3ad75e159d3dfd02201e",
        "docs/guides/insights.md",
        "the starved-from-zero diagnostics row's own subjects (the depth/utilization/live-workers split) — the stranded mention is the unserved split, subject-scoped",
    ),
    (
        "533d0eb32c11748e7d8b4a472bd0c329583d00f54d18b92f7c68fcbe9391de4c",
        "docs/guides/ops.md",
        "the ops deadline/queue-bootstrap row's own subject (the deadline sweep's own condition), not the gauge's reason set",
    ),
    (
        "ee5f1b5acac1c47524c539d61d3f44a43d9989a1ca782bebfb7621940be1a4d2",
        "tests/_audit_vendor/_old_leader_sweeps.py",
        "the frozen oracle's own span (subsumed by the whole-file entry above; kept for span traceability)",
    ),
    (
        "bea0ce54a1afe44dce7d961a7828f97ddfb1cecec1d06bea360daad266ba5435",
        "tests/_audit_vendor/_old_leader_sweeps.py",
        "the frozen oracle's own span (subsumed by the whole-file entry above; kept for span traceability)",
    ),
    (
        "71578f82d0279fa7814c5a781cdbbff27c48dbef63d0e8ced491a27d1d34cffc",
        "tests/_prom_review.py",
        "the real-worker probe's own scenario list (its own subjects: unserved queue / missing actor_config probes)",
    ),
    (
        "8e7a46427c8ca1fb5692e8515e7e60128d0eca72d5e9a013330c8af26ca76369",
        "tests/test_cli_doctor.py",
        "the doctor's own stated purpose quotes — the two-leg subject's true never-dispatches gloss",
    ),
    (
        "64753e245ef4e48e4df1540cbee5a54f22b43cd12abaf4422b2e5b7ed75cfad4",
        "tests/test_prometheus_metrics_review.py",
        "the sanctioned two-label subset regression fixture (layer-6 table A: kept as a subset control with separate third-reason coverage)",
    ),
    (
        "5efac33f8077342ca5f87425e8d7e0098910b134b293cd0357a5bce3f6feebc3",
        "docs/guides/testing.md",
        "the fake runner's run-until-drained refusal is about its own registration/stub mechanism, not the leader gauge (the admonition body is its own paragraph unit under the container model — the span is NARROWER than the old blank-line block, which also swallowed the `!!! note` marker line above the paragraph; the marker line is its own unit now)",
    ),
    (
        "92620b0d08dd0f15b25e36b084c243b5aeea2549497c41913b731070576dd5ba",
        "docs/guides/troubleshooting.md",
        "the stranded-jobs cause row's own subject-scoped text (the no-actor-config cause as the troubleshooting table states it)",
    ),
    (
        "415e5bac0cf540817600e9f34c21f3a4b249cc51339738d795f488609e62d136",
        "tests/test_reclaim_indefinite_deadline_handoff_attack.py",
        "the reclaim/deadline handoff attack's own subject (the schedule_to_close window's mechanics), not the gauge's reason set",
    ),
    (
        "1e12b6af12f97722719f6951216b9ca88f65688aef21e736ddb1ee8c220bc969",
        "tests/test_rt_orphans_stranded_detector_blind_spots.py",
        "the historical-base framing correction: the scoped/tensed text names the class in order to scope it",
    ),
    (
        "f04b00bedbf5b19e89449c169c8d9c245173c90d2d1413ca48579ab62bdda8e4",
        "tests/web_admin/test_queues.py",
        "the queue badge tooltip's own sanctioned two-leg text asserted by the page pin",
    ),
    (
        "24dc01ee8d8cf49c4e8567edb46d107d042aa31d762fc9b257a973f9d5e9d3f0",
        "docs/guides/retries.md",
        "the deadline sweep's own never-dispatched list item (expired pending jobs past schedule_to_close) — the deadline subject, not the gauge's reason set",
    ),
)

# ── the match units ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class Unit:
    """One semantic local unit: the text, its location, its source span.

    `dedent` is the container indentation the text was read past (the
    renderer's detab): a container body's units carry text dedented by
    the container's content column, so the no-truncation law
    reconstructs the expected text from the file's own raw lines minus
    exactly this many leading columns. Top-level units carry 0."""

    path: str
    kind: str
    start_line: int
    end_line: int
    text: str
    dedent: int = 0

    @property
    def span_digest(self) -> str:
        normalized = "\n".join(line.rstrip() for line in self.text.splitlines())
        # "surrogatepass" keeps the digest TOTAL on every unit the
        # scanner can build: a lone surrogate is a legal Python string
        # constant (a source escape such as "\ud800" survives ast.parse
        # into the constant's value), and a bare UTF-8 encode of it
        # raised UnicodeEncodeError instead of digesting (the round-8
        # D8-5 defect).
        return hashlib.sha256(normalized.encode("utf-8", "surrogatepass")).hexdigest()


#: The universal-gloss variants (rule 2) — with the scoping phrases that
#: make a unit PASS.
_UNIVERSAL_VARIANTS = (
    "can never dispatch",
    "can never be dispatched",
    "permanently undispatchable",
    "never dispatched",
    "never dispatch",
    "neither dispatch nor the deadline sweep",
    "no dispatch or deadline",
    "no dispatch round and no deadline sweep",
)

#: The scoping markers as a BOUNDED form (round-9 D9-6; round-10
#: F10-4): word-bounded, matched against the scope-normalized text
#: (`_scope_text`), and each alternative NAMES the two
#: dispatch-invisible legs themselves. The round-10 defect: the loose
#: alternative `two dispatch(?!e)` matched "The two dispatch queues
#: are `default` and `bulk`" — a sentence naming the dispatcher
#: pools, not the legs — and that marker waived a universal gloss in
#: another sentence of the same unit.
_SCOPE_MARKER_RE: Final = re.compile(
    r"(?<![\w-])"
    r"(?:dispatch-invisible"
    r"|dispatch invisible"
    r"|may dispatch"
    r"|may be dispatched)"
    r"(?![\w-])"
)

#: A scope marker is NOT a scope when a NEGATOR sits in its own
#: sentence before it (round-10 F10-4: negation is clause-bounded —
#: the same sentence, not a 20-character window; "…are not in any
#: sense at all what anyone would call dispatch-invisible: they can
#: never dispatch" put the negator outside the old window and passed).
_NEGATOR_RE: Final = re.compile(
    r"\b(?:not|never|neither|nor|without|cannot|can't|won't|isn't|aren't|n't)\b"
)

#: The sentence grid the scope predicate runs on (`_scope_text`'s
#: normalized text, split after sentence-ending punctuation ONLY — a
#: period, `!` or `?` followed by whitespace). Colons, semicolons and
#: dashes are NOT boundaries: the shipped scope sentences bind their
#: claims across them ("These two reasons are dispatch-invisible:
#: neither dispatch nor the deadline sweep will ever touch such
#: rows."; a parenthetical's semicolon must not amputate the sentence
#: from its own scope). A marker waives a UNIVERSAL occurrence only
#: within the occurrence's OWN sentence (round-10 F10-4: unit-level
#: scoping let a scope sentence waive a universal sentence three
#: sentences later); rules 1/3/4 are unit-level.
_SENTENCE_BOUNDARY: Final = re.compile(r"(?<=[.!?])\s+")

#: The truthful SUBJECT-SCOPED form (round-10 F10-4's false positive):
#: a universal variant whose grammatical subject is ONE reason's own
#: rows — a label and its noun immediately before the variant — is
#: that reason's own true meaning, not a whole-population gloss. A
#: label fragment earlier in the clause with a population subject in
#: between (a table cell's label followed by "these rows") is the
#: whole population and still reds. The tail alone is not the subject
#: (round-11 F11-2): a population subject EARLIER in the same sentence
#: ("all stranded entries like <label> rows …") is the claim's actual
#: subject — `_subject_scoped` requires no population subject anywhere
#: in the sentence before the occurrence, and `_label_bound` requires
#: it too unless the occurrence sits inside the last label's own
#: unclosed parenthetical (the exception is paren-bound and does not
#: verify subjecthood — the round-12 D12-3 declaration, residual 4).
_SUBJECT_SCOPED_TAIL: Final = re.compile(
    r"(?:no_actor_config|unserved_" + "queue|no actor_config)"
    r"\s+(?:rows?|jobs?|entries?|sources?|items?)\s*$"
)

#: The POPULATION SUBJECTS: a universal variant whose subject is the
#: gauge's whole population never waives by label proximity, and an
#: ANAPHORIC subject ("they can never dispatch") binds back to the
#: unit's own subject — the two-sentence laundering shape. The reach is
#: position-independent (round-11 F11-2): the WHOLE sentence before an
#: occurrence defeats the subject/label waivers, not only the text
#: between the last label and the claim — the label's own unclosed
#: parenthetical is the one exception (`_label_bound`), and that
#: exception is paren-bound, not subject-verified (round-12 D12-3:
#: residual 4 — a contrast that merely names the label hides the same
#: way, pinned as accepted).
_POPULATION_SUBJECT_RE: Final = re.compile(
    r"(?<![\w-])(?:(?:these|those|all|such)\s+(?:stranded\s+)?(?:rows?|jobs?|entries?)"
    r"|pending/scheduled"
    r"|stranded rows"
    r"|stranded jobs"
    r"|rows? that"
    r"|jobs? that)(?![\w-])"
)

#: The sentence-local DETECTOR mention: a universal variant whose own
#: sentence does not speak about the detector family at all, with a
#: specific (non-anaphoric, non-population) subject, is that sentence's
#: own restricted subject — "an actor that can never be dispatched
#: because it has no registered stub/config is a strictly worse failure
#: mode" — not a gloss on the gauge. The family's OWN canonical
#: spellings are mentions too (round-12 D12-1): `stranded-jobs` (the
#: shipped docs' hyphenated spelling) and the metric name
#: `taskq_jobs_stranded`. The old alternative list ended at bare
#: `stranded`, so the `(?![\w-])` lookahead rejected the hyphen and the
#: lookbehind rejected the underscore — a universal gloss about the
#: family in its own canonical spelling was treated as a foreign
#: restricted subject and waived. The alternatives stay word-bounded on
#: both ends, so a hyphenated or underscored compound beyond the name
#: (`stranded-jobs-no-actor-config`, `stranded-jobs-workflow-root-
#: missing`, `taskq_jobs_stranded_total`, `unstranded`, `non-stranded`)
#: still does not match.
_SENTENCE_MENTION_RE: Final = re.compile(
    r"(?<![\w-])(?:stranded(?:-jobs)?"
    r"|no_actor_config|unserved_queue|workflow_root_missing"
    r"|pending/scheduled"
    r"|taskq_jobs_stranded)(?![\w-])"
)

#: The PRONOUN SUBJECTS (the anaphoric refusal): a pronoun anywhere in
#: the head binds back to the unit's subject and never waives —
#: POSITION-INDEPENDENT (round-12 D12-2: the old check anchored the
#: pronoun to the head's end with `\s*$`, so the anaphora hid mid-head —
#: "They themselves <universal>" / "They in turn <universal>" waived
#: while "They <universal>" reds). The set is the ANAPHORIC PARADIGM —
#: the third-person personal/possessive/reflexive forms
#: (they/them/their/theirs/themselves, it/its/itself), the
#: first-person-plural voice the docs' own "we" sentences bind back
#: through (we/us/our/ours/ourselves), and the demonstratives
#: (these/those/all/such). Round-13 N13-1 widened it from the
#: seven-word set after the possessive/reflexive/plural forms were
#: demonstrated hiding ("Their rows <universal>.", "The gauge itself
#: <universal>.", "We <universal>."); the tree-wide scan stays green
#: over the widened set. A new pronoun
#: spelling is a re-review, not a silence (the same
#: declared-spelling-gap discipline as residual 4).
_PRONOUN_SUBJECT_RE: Final = re.compile(
    r"\b(?:they|them|their|theirs|themselves"
    r"|it|its|itself"
    r"|we|us|our|ours|ourselves"
    r"|these|those|all|such)\b"
)

#: A positive counter-claim in the same unit defeats the scope (the
#: round-9 D9-6 contrast defeat: "… can never dispatch; jobs on a
#: served queue can dispatch normally" — the contrast clause contradicts
#: the universal claim instead of scoping it) — UNLESS the
#: counter-claim's own sentence is the third leg's definition, which
#: names the third leg itself, whose OWN meaning is the row that CAN
#: dispatch when served (the shipped `_leader_sweeps.py` docstring and
#: runbooks prose: "reason=workflow_root_missing … Such a row CAN
#: dispatch and be bounded-consumed"). The waiver is sentence-bound —
#: it rescues a counter-claim inside the third leg's own definition
#: sentence and nothing else; a universal gloss in ANOTHER sentence of
#: the same unit still reds (the round-10 F10-4 negative: a unit that
#: names the third leg, states the scope, and then adds a
#: whole-population universal sentence).
_SCOPE_COUNTER_CLAIM_RE: Final = re.compile(
    r"(?<![\w-])(?:can dispatch"
    r"|dispatch(?:es)? normally"
    r"|is dispatchable"
    r"|are dispatchable)(?![\w-])"
)
_THIRD_LEG_NAMES: Final = (
    "workflow_root_missing",
    "workflow-root-missing",
    "workflowrootmissingerror",
    "workflow root missing",
    "the third leg",
    "the third reason",
    "the third meaning",
)

#: The parity/mirror claim variants (rule 3) — a whole-detector claim that
#: must be scoped to the two dispatch-invisible legs.
_PARITY_VARIANTS = (
    "same computation",
    "same question the same way",
    "mirrors the two shapes",
    "entire predicate",
    "the detector's shape",
    "the detector's sql shape",
    "detector's shape",
    "the whole detector",
    "the entire detector",
)

#: The rule-1/2/3 claim patterns, compiled word-bounded so a variant
#: can never match inside a longer word (and so `_claim_unwaived` can
#: report each occurrence's own position within its sentence). The
#: DEFINING phrases name the family's own framing vocabulary: a bare
#: detector mention ("the stranded-jobs detector surfaces it") is not
#: a definition — the detector-name forms are defining only in the
#: possessive-family-noun or appositive form ("the stranded detector's
#: rows:", "the stranded-jobs detector:").
_DEFINING_PHRASES = (
    "the whole set",
    "the entire",
    "the reason set",
    "the closed reason",
    "reasons are",
    "reason is",
    "the stranded-jobs check",
    "the stranded jobs check",
    "stranded-jobs check",
    "stranded-jobs (",
    "stranded jobs (",
    "that is the whole",
    "can never " + "dispatch",
    "can never be " + "dispatched",
    "that cannot make progress",
    "jobs that can never",
    "rows that can never",
    "stranded jobs",
    "stranded rows",
)
_DEFINING_DETECTOR_FORMS: Final = (
    r"the\s+stranded(?:-jobs)?\s+detector(?:'s)?\s+(?:own\s+)?"
    r"(?:reason|meaning|shape|check|population|rows?|jobs?|conditions?|set|class|gauge)s?\b",
    r"the\s+stranded(?:-jobs)?\s+detector\s*[:—-]",
)
_DEFINING_RE: Final = re.compile(
    r"(?<![\w-])(?:"
    + "|".join(re.escape(phrase) for phrase in _DEFINING_PHRASES)
    + "|"
    + "|".join(_DEFINING_DETECTOR_FORMS)
    + r")(?![\w-])"
)
_UNIVERSAL_RE: Final = re.compile(
    r"(?<![\w-])(?:"
    + "|".join(re.escape(variant) for variant in _UNIVERSAL_VARIANTS)
    + r")(?![\w-])"
)
_PARITY_RE: Final = re.compile(
    r"(?<![\w-])(?:" + "|".join(re.escape(variant) for variant in _PARITY_VARIANTS) + r")(?![\w-])"
)

#: The REQUIRED VOCABULARY HOMES (the positive-presence pin): every home
#: the ruling names must CARRY the third reason's spellings — deleting or
#: reverting any home reds this pin. The per-home requirements are the
#: spellings that home exists to state.
_REQUIRED_HOMES: tuple[tuple[str, tuple[str, ...], str], ...] = (
    (
        "docs/guides/runbooks.md",
        ("workflow_root_missing", "stranded-jobs-workflow-root-missing"),
        "the stranded-jobs runbook's third reason + its own event/remedy",
    ),
    (
        "docs/guides/ops.md",
        ("workflow_root_missing",),
        "the ops metric row + the no-knob row's third reason",
    ),
    (
        "docs/guides/workers.md",
        ("workflow_root_missing",),
        "the leader loop's detector description names all three reasons",
    ),
    (
        "docs/guides/workflows.md",
        ("workflow-root-missing",),
        "the fleet door's release reason + the attempted noop release prose",
    ),
    (
        "docs/taskqflow-coverage.md",
        ("workflow-root-missing",),
        "the shipped root-missing reason distinguished from the digest proposal",
    ),
    (
        "docs/api-reference/taskq.md",
        ("workflow-root-missing",),
        "the release-reason example beside its sibling",
    ),
    (
        "docs/guides/observability.md",
        ("workflow_root_missing",),
        "the canonical metric row's three meanings",
    ),
    (
        "docs/guides/insights.md",
        ("workflow_root_missing",),
        "the insights table's third-reason distinction",
    ),
    (
        "docs/guides/cli.md",
        ("workflow_root_missing",),
        "the doctor parity paragraph's scope names the third leg",
    ),
    (
        "docs/guides/admin-ui.md",
        ("workflow_root_missing",),
        "the Stranded column's scope names the third leg",
    ),
    ("docs/architecture.md", ("workflow_root_missing",), "the leader-detector's three shapes"),
    (
        "docs/guides/configuration.md",
        ("workflow_root_missing",),
        "the interval row's closed reason set",
    ),
    (
        "src/taskq/settings.py",
        ("workflow_root_missing",),
        "the interval Field description's closed reason set",
    ),
    (
        "src/taskq/worker/_leader_sweeps.py",
        ("workflow_root_missing", "stranded-jobs-workflow-root-missing"),
        "the detector's third leg + its own warning event",
    ),
    (
        "src/taskq/worker/run.py",
        ("workflow-root-missing", "dispatch-workflow-root-missing"),
        "the worker's own release arm + its log/event spellings",
    ),
    (
        "src/taskq/obs/_otel.py",
        ("workflow_root_missing",),
        "the StrandedReason type's third member + the gauge prose",
    ),
    (
        "src/taskq/workflows/_worker_execution.py",
        ("WorkflowRootMissingError",),
        "the executor's pass-through names the class",
    ),
    (
        "src/taskq/web/admin/queues.py",
        ("workflow_root_missing",),
        "the queue roll-up's scope comment names the third leg",
    ),
    (
        "src/taskq/contrib/prometheus/rules.yaml",
        ("workflow_root_missing",),
        "the shipped alert description's third reason",
    ),
    (
        "src/taskq/contrib/kubernetes/prometheus_rule.yaml",
        ("workflow_root_missing",),
        "the shipped k8s alert description's third reason",
    ),
)


#: The Markdown BLOCK PARSER. The docs build itself parses with
#: Python-Markdown (`markdown`, the lock's `mkdocs` dependency), but that
#: parser's block API produces an element tree with NO source positions
#: (`BlockParser.parseBlocks` works on blank-line-split chunks and no
#: block processor records a line map), so spans cannot come from it. The
#: installed-and-locked `markdown-it-py` (4.2.0 — locked via pip-audit's
#: `rich`, declared in the dev group this module's dependencies come
#: from) is a CommonMark block parser whose token stream carries an
#: exact `[start, end)` line map for every block token: paragraph,
#: heading (ATX and setext), thematic break, list items at any depth,
#: blockquotes (lazy continuation included), fenced AND indented code
#: blocks (including inside list items and MkDocs admonitions at any
#: container depth), HTML blocks and tables. Every block boundary below
#: is that token map's; no regex decides one.
_MD_PARSER: Final = MarkdownIt("default", {"html": True})

#: The block tokens whose CHILDREN own the content — a container is never
#: a unit itself (its leaf blocks are), so a container's span can never
#: fold a scoped sibling into a gloss: the sibling is a leaf block of
#: its own with its own span, at any container depth.
_CONTAINER_TOKENS: Final = frozenset(
    {"blockquote_open", "bullet_list_open", "ordered_list_open", "list_item_open"}
)

#: The leaf block tokens the unit model emits: a paragraph, a heading
#: (ATX or setext), a thematic break, a fenced or indented code block,
#: or an HTML block. Anything else — the parser growing a block
#: construct the unit model does not know — fails closed, never folds.
_LEAF_TOKENS: Final = frozenset(
    {"paragraph_open", "heading_open", "fence", "code_block", "html_block", "hr"}
)

#: A line-comment token inside a fenced or indented code block:
#: SQL/Lua `--`, C++/JS `//`, shell/YAML/Python `#`, LaTeX/Matlab `%`,
#: or Lisp `;`. A line comment runs to end of line, so adjacent comment
#: lines inside a code block are SIBLING statements, never folded (the
#: same law the YAML comment units encode). Block comments
#: (`/* ... */`, `<!-- ... -->`) are whole-statement units of their own
#: (`_code_comment_units`).
_FENCE_COMMENT_PREFIXES: Final = ("--", "//", "#", "%", ";")

#: A TRAILING line comment inside a fenced/indented code block: a
#: comment marker ANYWHERE in the line — "SELECT 1;  -- <gloss>" and
#: "SELECT 1;-- <gloss>" alike. The round-10 F10-3 defect: only
#: full-line comments got units, so a trailing gloss had no unit of its
#: own and a sibling scoped comment purified it (the fence's own coarse
#: unit carried both). The round-11 F11-1 defect: the old lookbehind
#: required whitespace before the marker, but `--` begins a SQL/Lua
#: line comment with NO preceding-whitespace requirement (`SELECT
#: 1;-- x` is valid SQL), and the same holds for `#` in Python, `//`
#: in the C family, `%` in LaTeX and `;` in Lisp — so a no-space
#: trailing gloss got no unit and was purified the same way. An
#: over-taken marker (a `//` inside a URL, a `#` in a string) can only
#: ADD a unit — an extra unit can only add a red, never hide one,
#: because the fence's own coarse unit is emitted unconditionally and
#: still carries the whole content.
_TRAILING_COMMENT_START: Final = re.compile(r"--|//|#|%|;")


def _normalize_newlines(text: str) -> str:
    """The parser's line grid: CR/CRLF folded to LF. Every line index in
    this module is a LF index (the round-9 D9-5 defeat: `str.splitlines`
    also splits on \\v, \\f, \\x85, U+2028 and U+2029, which markdown-it
    treats as CONTENT, so a unit's text truncated and text hid outside
    every unit)."""
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _split_lines(text: str) -> list[str]:
    """The file's lines on the parser's own grid — split on "\\n" ONLY
    (`str.splitlines` splits on eight more characters; D9-5). A trailing
    newline yields a final empty string, which is blank and therefore
    never a unit's text; the line COUNT agrees with markdown-it's and
    PyYAML's line numbers."""
    return _normalize_newlines(text).split("\n")


#: The renderer's `tab_length` (Python-Markdown's default): the tab-stop
#: interval its NormalizeWhitespace preprocessor expands every tab to
#: before any block processor runs.
_TAB_LENGTH: Final = 4


def _renderer_lines(raw: str) -> list[str]:
    """The RENDERER's own line grid for markdown: LF-only lines
    (`_split_lines`) with every tab expanded to the next `_TAB_LENGTH`
    tab stop — Python-Markdown's NormalizeWhitespace preprocessor
    (`source.expandtabs(tab_length)`) runs before any block processor,
    so on the renderer's grid every character is exactly one column and
    the container detab law's slice (`line[content_column:]`) removes
    the columns it claims. The round-10 F10-5 defect: the model sliced
    the RAW line by characters while the detab law counted a tab as
    four columns — a tab-indented body line's text was mangled and the
    no-truncation law shared the slice, so it could not see the mangle.
    The markdown model and the law now share THIS grid; `expandtabs` is
    idempotent, so the recursion re-expands nothing."""
    return [line.expandtabs(_TAB_LENGTH) for line in _split_lines(raw)]


#: The MkDocs CONTAINER grammars the docs build enables (`mkdocs.yml`:
#: `admonition`, `pymdownx.details`, `pymdownx.tabbed`). markdown-it has
#: no admonition/tab extension — a container's body parses as ONE
#: indented code block (or a paragraph's lazy continuation), so its
#: paragraphs fold into a single unit (the round-9 D9-1 regression:
#: docs/guides/actors.md's two body paragraphs became unit 145-160 and a
#: scoped paragraph purified a universal gloss; the docs carry 84
#: admonitions, 27 with a code block inside, 17 with multi-paragraph
#: bodies). The marker patterns below are the RENDERER'S OWN
#: block-processor regexes — `markdown/extensions/admonition.py` `RE`,
#: `pymdownx/details.py` `START`, `pymdownx/tabbed.py` `START` —
#: re-anchored per line with the container's indent captured; the body
#: law is the renderer's `BlockProcessor.detab`: every line after the
#: marker that is blank or indented >= marker column + 4, across blank
#: lines, ending at the first non-blank line indented less (the
#: renderer's `theRest`). The body is DEDENTED by column + 4 and
#: re-parsed as markdown at a line offset — the renderer's own
#: `parseChunk(div, block)` — so a body paragraph, list or fence is its
#: own unit at its true line, and a scoped sibling inside a container
#: can never purify a gloss sibling.
_ADMONITION_MARKER: Final = re.compile(r"^(\s*)!!! ?([\w\-]+(?: +[\w\-]+)*)(?: +\"(.*?)\")? *(?:$)")
_DETAILS_MARKER: Final = re.compile(
    r"^(\s*)\?{3}(\+)? ?(?:([\w\-]+(?: +[\w\-]+)*?)?(?: +\"(.*?)\")|([\w\-]+(?: +[\w\-]+)*?)) *(?:$)"
)
_TAB_MARKER: Final = re.compile(r"^(\s*)={3}(?:\+!|!\+|\+|!)? +\"(.*?)\" *(?:$)")


#: The blockquote prefix the renderer's own blockquote processor
#: strips — `BlockQuoteProcessor.RE = ^[ ]{0,3}>[ ]?(.*)` applied per
#: line and per nesting level (`clean` strips `>` and at most one
#: space; a `>`-only line cleans to the empty string).
_QUOTE_PREFIX: Final = re.compile(r"(?: {0,3}> ?)+")


def _indent_columns(text: str) -> int:
    """The leading-whitespace run's width in columns on the renderer's
    tab-expanded grid (every character one column)."""
    return len(text) - len(text.lstrip(" "))


def _container_marker(line: str) -> tuple[int, int] | None:
    """The line's (blockquote-prefix columns, container-marker indent)
    in columns, or None. The marker regexes are the RENDERER'S OWN
    block-processor regexes — `markdown/extensions/admonition.py` `RE`,
    `pymdownx/details.py` `START`, `pymdownx/tabbed.py` `START` —
    anchored on the line the way the renderer sees it, which for a
    blockquote-prefixed container (`> !!! note`) is AFTER the
    blockquote processor's clean step (`_QUOTE_PREFIX`) — the renderer
    strips the prefix per line and re-runs the block processors on the
    cleaned chunk, so a quoted marker IS a container at its cleaned
    column (the round-10 F10-2 defect: the quoted form was not a
    container at all and its body folded). A marker inside a
    fenced/indented code block or a raw HTML block never reaches here —
    `_markdown_units` only scans non-opaque lines — which is the
    renderer's own behaviour too (a `!!!` line inside code is code)."""
    match = _ADMONITION_MARKER.match(line) or _DETAILS_MARKER.match(line) or _TAB_MARKER.match(line)
    if match is not None:
        return 0, _indent_columns(match.group(1))
    quote = _QUOTE_PREFIX.match(line)
    if quote is None:
        return None
    cleaned = line[quote.end() :]
    match = (
        _ADMONITION_MARKER.match(cleaned)
        or _DETAILS_MARKER.match(cleaned)
        or _TAB_MARKER.match(cleaned)
    )
    if match is None:
        return None
    return len(quote.group()), _indent_columns(match.group(1))


def _container_body_end(
    lines: list[str], marker_index: int, column: int, quote_columns: int = 0
) -> int:
    """The renderer's detab body law: the body is every line after the
    marker that is blank or indented >= column + 4, across blank lines,
    ending at the first non-blank line indented less (that line is the
    renderer's `theRest` — ordinary blocks again). For a
    blockquote-prefixed container the law runs on the CLEANED lines —
    the renderer strips the blockquote prefix per line
    (`BlockQuoteProcessor.clean`: a `>`-only line becomes the empty
    string, `^[ ]{0,3}>[ ]?` loses one level, anything else passes
    through as a lazy line) and re-runs the block processors on the
    cleaned chunk, so a `>`-only separator line is a blank body line
    and the body law crosses it."""
    content_column = column + 4
    index = marker_index + 1
    while index < len(lines):
        line = lines[index]
        if quote_columns:
            quote = _QUOTE_PREFIX.match(line)
            cleaned = line[quote.end() :] if quote is not None else line
        else:
            cleaned = line
        if not cleaned.strip():
            index += 1
            continue
        if _indent_columns(cleaned) < content_column:
            break
        index += 1
    return index


def _opaque_line_spans(tokens: list[Token], path: str) -> list[tuple[int, int]]:
    """The merged 1-based inclusive line spans of the tokens whose
    content is LITERAL — fenced/indented code and raw HTML blocks. A
    `!!!`/`???`/`===` line inside one is code text, never a container
    marker (the renderer agrees: its admonition regex only matches
    block-start lines, and code processors claim those lines first)."""
    spans: list[tuple[int, int]] = []
    for token in tokens:
        if token.type in ("fence", "code_block", "html_block"):
            start, end = _span_lines(token, path)
            spans.append((start, end))
    spans.sort()
    merged: list[tuple[int, int]] = []
    for start, end in spans:
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _line_in_spans(line: int, spans: list[tuple[int, int]]) -> bool:
    return any(start <= line <= end for start, end in spans)


def _markdown_units(
    path: str, text: str, _line_offset: int = 0, _dedent: int = 0
) -> Iterator[Unit]:
    """Markdown BLOCK units from the real parser's token/line map, plus
    the MkDocs container bodies the parser cannot see, plus the per-line
    floor. The previous hand-rolled machines (blank-line blocks, then
    round-7's structural siblings) folded a new construct on every
    round; round 8 drove the block boundaries from markdown-it's token
    map and round 9 still found the fold — markdown-it has NO MkDocs
    admonition/tab extension, so a `!!! tip` body parsed as one
    `code_block` and two prose paragraphs became one unit (D9-1). The
    model is now three mechanisms, each total over its own grammar:

    * **blocks** — every leaf block is ONE unit at the parser's own
      span (paragraph, heading ATX/setext, thematic break, fenced or
      indented code block, HTML block); containers are never units
      (their leaf blocks are); tables emit per-row units;
    * **containers** — the docs build's `!!!`/`???`/`===` bodies are
      dedented and RE-PARSED as markdown at a line offset (the
      renderer's own detab law, `_container_body_end`); a marker inside
      a fenced/indented code block or a raw HTML block is code text,
      never a container (`_opaque_line_spans`);
    * **the floor** — every non-blank line no unit owns (a link-
      reference definition the parser consumes without a token, a
      `>`-only separator, any construct the model does not know) is its
      OWN per-line unit, never folded: the mechanism that makes the
      line-coverage law true. A construct the model does not know can
      no longer hide text — it degrades to per-line scanning.

    The additive refinements stay: each fenced/indented code block's
    comment statements are their own units, derived from the parser's
    de-prefixed `token.content` mapped back through the token's line
    map (quoting a fence cannot defeat them — D9-3), with `/* ... */`
    and `<!-- ... -->` block comments as whole-statement units (D9-4).

    A blockquote-prefixed container (`> !!! note`) IS a container: the
    marker is matched after the renderer's blockquote clean law
    (`_QUOTE_PREFIX`) and the same body law runs on the cleaned lines,
    so the quoted form gets the same body paragraph units as the
    unquoted form (the round-10 F10-2 defect: the quoted body was left
    at the blockquote's paragraph granularity and ONE unit folded its
    two body paragraphs, which the renderer renders as two `<p>`s).
    A `>`-only separator line inside a body is a FLOOR unit (its own
    line, `markdown-text`) — the body law crosses it as the renderer's
    blank-line block separator does. And an unclosed fence runs to
    end of file — in the RENDERER too (CommonMark), so the model is
    renderer-faithful; the swallowed text stays inside the fence's own
    unit, where the rules still evaluate it."""
    lines = _renderer_lines(text)
    tokens = _MD_PARSER.parse("\n".join(lines))
    opaque = _opaque_line_spans(tokens, path)
    units: list[Unit] = []
    run_start = 0
    index = 0
    while index < len(lines):
        line_no = index + 1
        marker = None if _line_in_spans(line_no, opaque) else _container_marker(lines[index])
        if marker is None:
            index += 1
            continue
        quote_columns, marker_column = marker
        body_end = _container_body_end(lines, index, marker_column, quote_columns)
        if index > run_start:
            run_text = "\n".join(lines[run_start:index])
            units.extend(_markdown_run_units(path, run_text, run_start + _line_offset, _dedent))
        # The marker line is its own unit (the container's title
        # statement — the renderer renders it as the admonition-title
        # paragraph). Its text is the line as the container's own
        # grammar sees it: the blockquote prefix cleaned away, and the
        # unit's dedent carries the columns the law must strip from the
        # file's raw line to reconstruct it.
        units.append(
            Unit(
                path,
                "markdown",
                line_no + _line_offset,
                line_no + _line_offset,
                lines[index][quote_columns:],
                _dedent + quote_columns,
            )
        )
        # The body: dedent by the container's content column (the
        # renderer's detab) past the blockquote prefix, re-parse as
        # markdown at the body's own line offset.
        content_column = marker_column + 4
        body_text = "\n".join(
            "" if not line.strip() else line[quote_columns + content_column :]
            for line in lines[index + 1 : body_end]
        )
        units.extend(
            _markdown_units(
                path,
                body_text,
                _line_offset=index + 1 + _line_offset,
                _dedent=_dedent + quote_columns + content_column,
            )
        )
        index = body_end
        run_start = index
    if run_start < len(lines):
        run_text = "\n".join(lines[run_start:])
        units.extend(_markdown_run_units(path, run_text, run_start + _line_offset, _dedent))
    # The per-line floor: the coverage law's own mechanism.
    covered: set[int] = set()
    for unit in units:
        covered.update(range(unit.start_line, unit.end_line + 1))
    for number, line in enumerate(lines, 1):
        if number + _line_offset not in covered and line.strip():
            units.append(
                Unit(
                    path,
                    "markdown-text",
                    number + _line_offset,
                    number + _line_offset,
                    line.strip(),
                )
            )
    yield from units


def _markdown_run_units(path: str, text: str, line_offset: int, dedent: int = 0) -> Iterator[Unit]:
    """One non-container run's units: the run's own parse (the parser's
    token map is correct for a run that contains no container marker),
    offset to the run's absolute lines, carrying the run's cumulative
    container dedent. The run's lines come from the renderer's
    tab-expanded grid (`_renderer_lines` — idempotent on the already
    expanded run text the container recursion passes down)."""
    lines = _renderer_lines(text)
    for block in _markdown_block_tree(_MD_PARSER.parse(text)):
        yield from _markdown_block_units(path, lines, block, line_offset, dedent)


@dataclass
class _Block:
    """One parsed block token re-nested with its parsed children (the
    markdown-it token stream is a flat depth-first list; this is its
    block tree)."""

    token: Token
    children: list[_Block] = field(default_factory=list)


def _markdown_block_tree(tokens: list[Token]) -> list[_Block]:
    """Re-nest the parser's flat depth-first token stream (each token's
    `nesting` is 1 open / -1 close / 0 standalone). The parser
    guarantees the open/close pairing; a malformed stream is a
    fail-closed IndexError, never a guessed boundary."""
    roots: list[_Block] = []
    stack: list[list[_Block]] = [roots]
    for token in tokens:
        if token.nesting == -1:
            stack.pop()
            continue
        block = _Block(token=token)
        stack[-1].append(block)
        if token.nesting == 1:
            stack.append(block.children)
    return roots


def _span_lines(token: Token, path: str) -> tuple[int, int]:
    """The token's map as 1-based inclusive line bounds, fail-closed on
    an unmapped block token."""
    if token.map is None:
        raise AssertionError(
            f"{path}: the parser emitted block token {token.type!r} without a "
            "source map — fail closed, never guessed"
        )
    return token.map[0] + 1, token.map[1]


def _markdown_block_units(
    path: str, lines: list[str], block: _Block, line_offset: int = 0, dedent: int = 0
) -> Iterator[Unit]:
    """One parsed block's units: containers recurse into their children
    (a container is never a unit), the table is emitted per row, every
    other leaf block is one unit at the parser's own span — with the
    code blocks' comment statements and the HTML blocks' element runs
    added as their own units."""
    token = block.token
    if token.type in _CONTAINER_TOKENS:
        for child in block.children:
            yield from _markdown_block_units(path, lines, child, line_offset, dedent)
        return
    start, end = _span_lines(token, path)
    start += line_offset
    end += line_offset
    if token.type == "table_open":
        yield from _table_row_units(path, lines, start, end, line_offset, dedent)
        return
    if token.type == "html_block":
        # The .md html_block gets the .html element/comment model INSTEAD
        # of a whole-span unit (round-9 D9-2: ONE html_block token folded
        # sibling `<tr>` rows and sibling wrapped comments; the element
        # model separates them — a whole-span unit here would fold them
        # again).
        span_text = "\n".join(lines[start - 1 - line_offset : end - line_offset])
        yield from _html_units(path, span_text, line_offset=start - 1, dedent=dedent)
        return
    if token.type not in _LEAF_TOKENS:
        raise AssertionError(
            f"{path}: the parser emitted block token {token.type!r} the unit model "
            "does not know — fail closed, never folded"
        )
    yield Unit(
        path,
        "markdown",
        start,
        end,
        "\n".join(lines[start - 1 - line_offset : end - line_offset]),
        dedent,
    )
    if token.type in ("fence", "code_block"):
        yield from _code_comment_units(path, lines, token, start, end, line_offset, dedent)


def _code_comment_units(
    path: str,
    lines: list[str],
    token: Token,
    start: int,
    end: int,
    line_offset: int,
    dedent: int = 0,
) -> Iterator[Unit]:
    """The comment statements INSIDE a fenced or indented code block,
    detected on the parser's own DE-PREFIXED `token.content` and mapped
    back through the token's line map (the round-9 D9-3 defeat: the raw
    lines inside a blockquote still carry the `> ` prefix, `.strip()`
    never removes it, so a quoted fence's comment lines had no units and
    its adjacent comment pair folded back into one unit — quoting a
    fence re-opened round-7's defeat). A line comment runs to end of
    line: each comment LINE is its own unit — a FULL-LINE comment
    (`--`, `//`, `#`, `%`, `;`) and a TRAILING comment after code
    (`SELECT 1;  -- <gloss>` and `SELECT 1;-- <gloss>` alike — the
    marker needs no preceding whitespace; the round-10 F10-3 defect
    left the trailing form inside the fence's coarse unit, where a
    sibling scoped comment purified it, and the round-11 F11-1 defect
    left the no-whitespace form there the same way) alike. A `/* ... */` or
    `<!-- ... -->` block comment is ONE statement: the whole statement
    is its own unit, a multi-line comment staying the one unit it is
    (the round-9 D9-4 defeat: only line-comment prefixes were known, so
    a block-comment gloss beside a scoped line comment had no unit).
    The unit's text is the file's own raw lines over the mapped span
    (prefixes and indents included — they never hide a match)."""
    content_lines = token.content.split("\n")
    if content_lines[-1] == "":
        content_lines.pop()
    span_lines = end - start + 1
    if token.type == "fence":
        # A closed fence's span is opening + content + closing; an
        # unclosed fence has no closing line. Any other shape fails
        # closed.
        if len(content_lines) in (span_lines - 2, span_lines - 1):
            first_content = start + 1
        else:
            raise AssertionError(
                f"{path}: the fence's content has {len(content_lines)} lines for "
                f"span {start}-{end} — fail closed, never guessed"
            )
    else:  # indented code block: every span line is a content line
        if len(content_lines) != span_lines:
            raise AssertionError(
                f"{path}: the indented code block's content has "
                f"{len(content_lines)} lines for span {start}-{end} — fail closed"
            )
        first_content = start
    # Block comments first: whole statements, mapped through the content
    # line numbers (a multi-line comment stays ONE unit).
    content = token.content
    for match in re.finditer(r"/\*.*?\*/|<!--.*?-->", content, re.DOTALL):
        first_line = content.count("\n", 0, match.start())
        last_line = content.count("\n", 0, match.end())
        rel_lo = first_content + first_line - line_offset
        rel_hi = first_content + last_line - line_offset
        yield Unit(
            path,
            "markdown",
            first_content + first_line,
            first_content + last_line,
            "\n".join(lines[rel_lo - 1 : rel_hi]),
            dedent,
        )
    for index, content_line in enumerate(content_lines):
        stripped = content_line.strip()
        # A TRAILING comment (text after the code prefix) is its own
        # statement too — the round-10 F10-3 defect left it inside the
        # fence's coarse unit, where a sibling scoped comment purified it.
        if not stripped.startswith(_FENCE_COMMENT_PREFIXES) and (
            _TRAILING_COMMENT_START.search(content_line) is None
        ):
            continue
        raw_index = first_content + index - line_offset
        yield Unit(
            path,
            "markdown",
            first_content + index,
            first_content + index,
            lines[raw_index - 1],
            dedent,
        )


def _table_row_units(
    path: str,
    lines: list[str],
    start: int,
    end: int,
    line_offset: int = 0,
    dedent: int = 0,
) -> Iterator[Unit]:
    """Every line of the parser's own table span is its own row unit
    (the round-4 law). The delimiter row and the header are units too —
    they carry no detector vocabulary and the rules never fire on them,
    but their spans stay addressable for the allowlist's digest law."""
    for idx in range(start, end + 1):
        text = lines[idx - 1 - line_offset].strip()
        if text:
            yield Unit(path, "markdown", idx, idx, text, dedent)


def _yaml_scalar_units(path: str, text: str) -> Iterator[Unit]:
    """YAML scalar units at the COMPOSER'S OWN node marks (parsed; the
    exact line span of every scalar, keys and values both). The previous
    heuristic searched for the scalar's first 40 characters and reported
    the scalar at whatever line it first matched — usually wrong for a
    folded scalar, leaving the folded body's lines outside every unit
    (the round-9 D9-7 defeat: 718 non-blank lines across the two alert
    files). A folded scalar's span is the composer's own `start_mark` /
    `end_mark`; a block scalar ending at a line's column 0 ends on the
    PREVIOUS line."""
    try:
        import yaml
    except ImportError:  # pragma: no cover - the dependency ships with the test env
        return
    try:
        documents = list(yaml.compose_all(text))
    except Exception as exc:
        # Fail closed: an unreadable YAML is red, never skipped.
        raise AssertionError(f"{path}: unreadable YAML ({exc})") from exc
    line_count = len(_split_lines(text))

    def walk(node: Any) -> Iterator[Any]:
        if isinstance(node, yaml.MappingNode):
            # The KEYS are scalars too — a key is a string the YAML
            # grammar parses, and a gloss carried in a key is just as
            # much a statement as one in a value.
            for key, value in node.value:
                yield from walk(key)
                yield from walk(value)
        elif isinstance(node, yaml.SequenceNode):
            for value in node.value:
                yield from walk(value)
        else:
            yield node

    for node in (walked for document in documents for walked in walk(document)):
        if not isinstance(node, yaml.ScalarNode) or not isinstance(node.value, str):
            continue
        start_line = node.start_mark.line + 1
        end_line = node.end_mark.line + 1 if node.end_mark.column > 0 else node.end_mark.line
        yield Unit(path, "yaml", start_line, min(end_line, line_count), node.value)


def _yaml_comment_units(path: str, text: str) -> Iterator[Unit]:
    """YAML comment units — each comment is its own unit, at the line the
    language scopes it to (a YAML comment runs to end of line, so
    adjacent `#` lines are SIBLING statements, never folded: a scoped
    sibling must not purify a gloss line — the round-6 M-1 defeat folded
    two adjacent comment lines into one blank-line block and the gloss
    stayed green). Full-line and trailing comments both get their own
    unit; a `#` inside a quoted scalar may be over-taken as a comment —
    an extra unit can only add a red, never hide one, and the scalar
    itself stays its own parsed unit either way."""
    for idx, line in enumerate(_split_lines(text), start=1):
        match = re.search(r"(?:(?<=\s)|^)#", line)
        if match is not None:
            yield Unit(path, "yaml-comment", idx, idx, line[match.start() :].strip())


#: The HTML void elements — a start tag for one of these is a complete
#: element (no end tag exists to close it).
_HTML_VOID_ELEMENTS: Final = frozenset(
    {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
)


class _HTMLTemplateParser(HTMLParser):
    """Collects element and comment token offsets from a template. Tolerant
    of the Jinja statement grammar the HTML grammar sees as text (`{% %}` /
    `{{ }}` ride the surrounding units); never rejects template markup."""

    def __init__(self, line_starts: list[int]) -> None:
        super().__init__(convert_charrefs=False)
        self._line_starts = line_starts
        self.tokens: list[tuple[str, int, str]] = []

    def _offset(self) -> int:
        line, col = self.getpos()
        return self._line_starts[line - 1] + col

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tokens.append(("start", self._offset(), tag))

    def handle_endtag(self, tag: str) -> None:
        self.tokens.append(("end", self._offset(), tag))

    def handle_comment(self, data: str) -> None:
        self.tokens.append(("comment", self._offset(), data))


def _comment_statement_units(
    path: str, text: str, line_offset: int = 0, dedent: int = 0
) -> Iterator[Unit]:
    """The comment STATEMENTS of a template/markup text: every
    `<!-- -->` HTML comment and every `{# ... #}` Jinja/Tera comment is
    its own unit, a multi-line comment staying the one statement it is
    (the .html template units and the .md files' additive comment
    treatment — the round-8 D8-4 defeat left `.md` comments outside the
    element run units and two adjacent comments folded)."""
    line_starts = [0] + [match.end() for match in re.finditer("\n", text)]

    def line_of(offset: int) -> int:
        return bisect.bisect_right(line_starts, offset) + line_offset

    parser = _HTMLTemplateParser(line_starts)
    try:
        parser.feed(text)
        parser.close()
    except AssertionError as exc:  # fail closed: an unparseable template is red
        raise AssertionError(f"{path}: unparseable HTML ({exc})") from exc

    # The comment statements (exact raw spans).
    for kind, offset, _data in parser.tokens:
        if kind != "comment" or not text.startswith("<!--", offset):
            continue
        close = text.find("-->", offset + 4)
        end = len(text) if close == -1 else close + 3
        yield Unit(path, "html-comment", line_of(offset), line_of(end), text[offset:end], dedent)
    for match in re.finditer(r"\{#.*?#\}", text, re.DOTALL):
        yield Unit(
            path,
            "template-comment",
            line_of(match.start()),
            line_of(match.end()),
            match.group(),
            dedent,
        )


def _html_units(path: str, text: str, line_offset: int = 0, dedent: int = 0) -> Iterator[Unit]:
    """HTML/Jinja/Tera template units — the ruling's "templates and UI
    text" surface. Each element's OWN run is a unit: a leaf element is
    one unit (a `<tr>` row, a `<span>` badge — a wrapped element stays
    ONE unit, never fragmented), and a container contributes its own
    runs (its start tag + direct text, the text between child elements,
    its end tag) while each CHILD element is its own unit — so sibling
    elements are separate units and a scoping marker in one can never
    purify a sibling's gloss (the round-6 M-1 defeat folded two adjacent
    `<tr>` rows into one blank-line block and the gloss stayed green).
    Every `<!-- -->` HTML comment and every `{# ... #}` Jinja/Tera
    comment is its own unit, a multi-line comment staying the one
    statement it is. The text no element or comment owns (Jinja control
    lines, a doctype, a stray end tag) is its own PER-LINE unit — the
    previous markdown floor's blank-line blocks folded non-blank
    template text into one unit (a template is not markdown; the fold
    was the laundering shape this model exists to prevent), so the
    floor is now the element/comment grammar's own gap coverage, line
    by line, never folded. `line_offset` relocates every unit to the
    span's absolute lines (the .md html_block call — the round-9
    D9-2 defeat)."""
    line_starts = [0] + [match.end() for match in re.finditer("\n", text)]

    def line_of(offset: int) -> int:
        return bisect.bisect_right(line_starts, offset) + line_offset

    parser = _HTMLTemplateParser(line_starts)
    try:
        parser.feed(text)
        parser.close()
    except AssertionError as exc:  # fail closed: an unparseable template is red
        raise AssertionError(f"{path}: unparseable HTML ({exc})") from exc

    # The Jinja/Tera comment regions: the HTML grammar sees a tag INSIDE
    # a `{# ... #}` comment (`an empty <i> ...`) as a REAL element, which
    # split the surrounding own-runs and crossed the comment's own unit
    # (the shipped _base.html carries exactly that). A token inside a
    # Jinja comment region is comment TEXT, never an element token.
    jinja_regions = [
        (match.start(), match.end()) for match in re.finditer(r"\{#.*?#\}", text, re.DOTALL)
    ]

    def in_jinja_region(offset: int) -> bool:
        return any(start <= offset < end for start, end in jinja_regions)

    parser.tokens = [token for token in parser.tokens if not in_jinja_region(token[1])]

    # Element spans with implied ends: a repeated start tag implies the
    # end of the previous open one; an end tag closes every element open
    # above its match; EOF closes the rest.
    boundaries = sorted({offset for _kind, offset, _tag in parser.tokens} | {len(text)})
    open_stack: list[tuple[str, int]] = []
    elements: list[tuple[int, int]] = []

    def next_boundary_after(offset: int) -> int:
        return boundaries[bisect.bisect_right(boundaries, offset)]

    for kind, offset, tag in parser.tokens:
        if kind == "start":
            if tag in _HTML_VOID_ELEMENTS:
                elements.append((offset, next_boundary_after(offset)))
                continue
            if any(open_tag == tag for open_tag, _ in open_stack):
                while open_stack:
                    open_tag, open_start = open_stack.pop()
                    elements.append((open_start, offset))
                    if open_tag == tag:
                        break
            open_stack.append((tag, offset))
        elif kind == "end":
            depth = next((i for i, (open_tag, _) in enumerate(open_stack) if open_tag == tag), None)
            if depth is None:
                continue  # a stray end tag closes nothing
            while len(open_stack) > depth:
                _open_tag, open_start = open_stack.pop()
                elements.append((open_start, offset))
    for _open_tag, open_start in open_stack:
        elements.append((open_start, len(text)))

    # The non-overlapping own-runs: each element's span minus its direct
    # children's spans.
    elements.sort(key=lambda span: (span[0], -span[1]))
    children: dict[tuple[int, int], list[tuple[int, int]]] = {}
    ancestors: list[tuple[int, int]] = []
    for span in elements:
        while ancestors and ancestors[-1][1] <= span[0]:
            ancestors.pop()
        if ancestors:
            children.setdefault(ancestors[-1], []).append(span)
        ancestors.append(span)
    units: list[Unit] = []
    for start, end in elements:
        cursor = start
        for child_start, child_end in children.get((start, end), ()):
            if child_start > cursor:
                units.append(
                    Unit(
                        path,
                        "html-element",
                        line_of(cursor),
                        line_of(child_start),
                        text[cursor:child_start],
                        dedent,
                    )
                )
            cursor = max(cursor, child_end)
        if cursor < end:
            units.append(
                Unit(
                    path,
                    "html-element",
                    line_of(cursor),
                    line_of(end),
                    text[cursor:end],
                    dedent,
                )
            )
    units.extend(_comment_statement_units(path, text, line_offset, dedent))

    # The gap floor: every line no element own-run and no comment
    # statement owns is its own template-statement unit — per LINE,
    # never folded.
    covered: set[int] = set()
    for unit in units:
        covered.update(range(unit.start_line, unit.end_line + 1))
    for idx, line in enumerate(_split_lines(text), start=1):
        if idx + line_offset not in covered and line.strip():
            units.append(
                Unit(
                    path,
                    "html-text",
                    idx + line_offset,
                    idx + line_offset,
                    line.strip(),
                    dedent,
                )
            )
    yield from units


def _python_units(path: str, text: str, tree: ast.Module) -> Iterator[Unit]:
    """AST docstrings, each comment line its own unit (tokenize follows
    every comment token with the next non-COMMENT token — an NL after a
    full-line comment and after a trailing comment inside an open
    bracket, where the logical line continues, a NEWLINE after a
    trailing comment that ends the logical line — and the unit
    block flushes on that next token, so two adjacent comment lines are
    sibling units, never folded), `Field(description=...)`
    strings, and the typed collection literals (rule 4's own units). A
    docstring is ONE unit: the AST walk sees the same Constant as both
    the docstring and a string constant, so the duplicate `string` unit
    (identical span and text) is dropped — the round-9 D9-8 defect had
    every docstring reported twice."""
    units: list[Unit] = []
    # The f-string parts: a JoinedStr's inner Constants report PEP-701
    # concat-spanning positions that PARTIALLY OVERLAP at line
    # granularity (adjacent chunks share the boundary line). The
    # f-string's literal text is ONE unit instead — the resolved parts
    # joined — carrying the same words the chunk units gave, without the
    # boundary artifacts.
    joined_part_ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            for part in node.values:
                if isinstance(part, ast.Constant):
                    joined_part_ids.add(id(part))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                doc_node = (
                    node.body[0]
                    if getattr(node, "body", None) and isinstance(node.body[0], ast.Expr)
                    else None
                )
                # The docstring unit's span is the DOC NODE'S OWN SOURCE
                # span (the value's splitlines count drops a trailing
                # empty line, which under-covered the closing-quotes
                # line and partially overlapped the string unit's own
                # span).
                first = doc_node.lineno if doc_node is not None else getattr(node, "lineno", 1)
                last = (doc_node.end_lineno or first) if doc_node is not None else first
                units.append(Unit(path, "docstring", first, last, doc))
        if isinstance(node, ast.JoinedStr):
            parts = [
                part.value
                for part in node.values
                if isinstance(part, ast.Constant) and isinstance(part.value, str)
            ]
            literal = "".join(parts)
            units.append(
                Unit(
                    path,
                    "string",
                    node.lineno,
                    node.end_lineno or node.lineno,
                    literal,
                )
            )
        if isinstance(node, ast.Constant) and isinstance(node.value, (str, bytes)):
            if id(node) in joined_part_ids:
                continue
            # Every string constant is a unit (Field(description=...) strings
            # included — the constant arm catches them; no parent chain is
            # needed and the AST is walked without parent linking). A BYTES
            # literal is a text carrier too (the tokenize STRING token the
            # ownership law pins): its text is decoded leniently.
            value = (
                node.value
                if isinstance(node.value, str)
                else node.value.decode("utf-8", "backslashreplace")
            )
            if node.lineno == node.end_lineno:
                units.append(Unit(path, "string", node.lineno, node.lineno, value))
            else:
                units.append(
                    Unit(path, "string", node.lineno, node.end_lineno or node.lineno, value)
                )
        # Rule 4's structural enumerations: Literal[...] subscripts and
        # tuple/list/set literals whose elements are the reason labels.
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id == "Literal"
        ):
            seg = ast.get_source_segment(text, node) or ""
            units.append(
                Unit(path, "literal-type", node.lineno, node.end_lineno or node.lineno, seg)
            )
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            seg = ast.get_source_segment(text, node) or ""
            units.append(Unit(path, "collection", node.lineno, node.end_lineno or node.lineno, seg))

    docstring_spans = {(u.start_line, u.end_line, u.text) for u in units if u.kind == "docstring"}
    yield from (
        unit
        for unit in units
        if unit.kind != "string"
        or (unit.start_line, unit.end_line, unit.text) not in docstring_spans
    )

    # Each comment LINE is its own unit (the AST carries comments only
    # via tokenize; every COMMENT token is followed by an NL/NEWLINE,
    # which flushes the one-comment block — two adjacent comment lines
    # are sibling units, never folded).
    import io
    import tokenize

    comment_block: list[tuple[int, str]] = []
    for token in tokenize.generate_tokens(io.StringIO(text).readline):
        if token.type == tokenize.COMMENT:
            comment_block.append((token.start[0], token.string))
        else:
            if comment_block:
                yield Unit(
                    path,
                    "comment",
                    comment_block[0][0],
                    comment_block[-1][0],
                    "\n".join(t for _, t in comment_block),
                )
                comment_block = []
    if comment_block:
        yield Unit(
            path,
            "comment",
            comment_block[0][0],
            comment_block[-1][0],
            "\n".join(t for _, t in comment_block),
        )


# ── the four rules ──────────────────────────────────────────────────────


def _lowered(unit: Unit) -> str:
    """The rule-matching form: case-folded with the markdown backticks
    removed (``no ``actor_config`` row`` must match the label form — the
    prose/backtick spelling can never defeat the match)."""
    return unit.text.lower().replace("`", "")


def _unit_mentions_detector(unit: Unit) -> bool:
    """The unit purports to speak about the stranded detector/gauge/reason
    family (the class's own context): its own names, its own closed
    labels, or its own pending/scheduled population."""
    lowered = _lowered(unit)
    return any(
        marker in lowered
        for marker in (
            "stranded",
            "no_actor_config",
            "unserved_queue",
            "workflow_root_missing",
            "pending/scheduled",
            "pending/schedu" + "led jobs",
        )
    )


def _scope_text(unit: Unit) -> str:
    """The scope predicate's own normalized form: case-folded, backticks
    and emphasis runs removed, whitespace collapsed — so ``**not**
    dispatch-invisible`` negates exactly like its plain form (markdown
    emphasis must never decide a scope)."""
    return re.sub(r"\s+", " ", _lowered(unit).replace("*", ""))


def _sentences(scope_text: str) -> list[str]:
    """The scope text's sentence segments (`_SENTENCE_BOUNDARY`) — the
    granularity a scope marker, a negator, or a counter-claim binds
    within."""
    return [sentence for sentence in _SENTENCE_BOUNDARY.split(scope_text) if sentence.strip()]


def _sentence_scoped(sentence: str, claim_spans: list[tuple[int, int]] = ()) -> bool:
    """The sentence carries a GENUINE scope marker: a marker is
    word-bounded and names the dispatch-invisible legs (`_SCOPE_MARKER_RE`),
    a negator in the same sentence before it kills it (`_NEGATOR_RE` —
    clause-bounded negation; a negator INSIDE a claim occurrence is the
    claim's own words — the "never" of "can never be dispatched" — and
    never a negation of the scope), and a positive counter-claim in the
    same sentence contradicts it — unless that sentence is the third
    leg's own definition, which names the third leg itself
    (`_THIRD_LEG_NAMES`): its can-dispatch clause is the third leg's OWN
    meaning, not a contradiction of the scope. The waiver is
    sentence-bound and rescues nothing else: a universal gloss in
    another sentence of the same unit finds no genuine marker in its
    own sentence and reds."""
    for match in _SCOPE_MARKER_RE.finditer(sentence):
        head = sentence[: match.start()]
        if any(
            not any(cs <= neg.start() < ce for cs, ce in claim_spans)
            for neg in _NEGATOR_RE.finditer(head)
        ):
            continue
        if _SCOPE_COUNTER_CLAIM_RE.search(sentence) is not None and not any(
            name in sentence for name in _THIRD_LEG_NAMES
        ):
            continue
        return True
    return False


def _subject_scoped(sentence: str, position: int) -> bool:
    """The claim occurrence at `position` is the predicate of a subject
    that IS one reason's own rows (`_SUBJECT_SCOPED_TAIL` immediately
    before it) and the sentence before the occurrence carries no
    population subject anywhere (`_POPULATION_SUBJECT_RE`). The
    round-11 F11-2 defect checked the tail alone: "all stranded entries
    like <label> rows …" matched the tail while the sentence's actual
    subject was the population — a population subject anywhere in the
    sentence before the occurrence is the claim's subject and defeats
    the waiver."""
    head = sentence[:position]
    if _POPULATION_SUBJECT_RE.search(head) is not None:
        return False
    return _SUBJECT_SCOPED_TAIL.search(head) is not None


def _label_bound(sentence: str, position: int) -> bool:
    """The occurrence is bound to ONE reason's own definition: a
    specific label occurs in the occurrence's sentence before it, with
    no population subject between the label and the occurrence
    ("unserved_queue (the queue dispatch routes the actor on has no
    live worker subscribed — likewise <universal>)" — the shipped
    alert descriptions' elliptical parenthetical). The round-11 F11-2
    defect checked only that between-text: a population subject BEFORE
    the label ("all stranded entries like <label> rows <universal>")
    was invisible and the whole-population gloss waived. A population
    subject anywhere in the sentence before the occurrence therefore
    defeats the waiver too — with one exception, the shipped framing's
    own shape: the occurrence sits inside the last label's own
    UNCLOSED parenthetical. The round-12 D12-3 correction states that
    exception's true reach: it is PAREN-BOUND, not subject-verified —
    the code checks only that the `(` count after the last label
    EXCEEDS the `)` count (a close-heavy unbalanced tail is not the
    exception — round-13 N13-6) and never that the label is the
    claim's subject, so a
    sentence whose subject is the population and whose label is only
    mentioned ("All stranded entries, unlike <label> (<universal>." —
    "All stranded entries share the <label> defect (so they are
    <universal>.") hides the same way; those three shapes are pinned
    as the accepted controls (residual 4). Accepted because the
    exception's only shipped users are the two alert descriptions'
    parentheticals — where the parenthetical IS each label's own
    meaning (deleting the exception false-reds exactly those two
    spans) — and a lexical check that the label is the claim's subject
    would hand-tune to the shipped wording. A population subject INSIDE
    that parenthetical still defeats the waiver (the between-text check
    runs first — a gloss inside the label's own parentheses reds).
    Round-13 N13-5 pins that red as the exception's WIDENING tripwire:
    a widening mutant that drops the between-text check passes every
    other corpus pin and the tree scan, so the inside-parenthetical
    corpus control is the only thing that reds it."""
    head = sentence[:position]
    labels = list(re.finditer("no_actor_config|unserved_" + "queue|no actor_config", head))
    if not labels:
        return False
    after = head[labels[-1].end() :]
    if _POPULATION_SUBJECT_RE.search(after) is not None:
        return False
    if _POPULATION_SUBJECT_RE.search(head) is None:
        return True
    return after.count("(") > after.count(")")


def _foreign_subject(sentence: str, position: int) -> bool:
    """The occurrence's own sentence does not speak about the detector
    family at all (`_SENTENCE_MENTION_RE`) and its subject is specific —
    not empty, anaphoric, or a population subject: "an actor that
    <universal> because it has no registered stub/config is a strictly
    worse failure mode" is that sentence's own restricted subject, not
    a gloss on the gauge. A pronoun in `_PRONOUN_SUBJECT_RE`'s set (the
    anaphoric paradigm: third-person personal/possessive/reflexive,
    first-person plural, demonstratives — round-13 N13-1 widened the
    set after their/its/itself/theirs/we/us/our/ours/ourselves/
    themselves were demonstrated hiding) or population subject binds
    back to the unit's subject and never waives — the pronoun refusal
    is POSITION-INDEPENDENT (`_PRONOUN_SUBJECT_RE` anywhere in the
    head; the round-12 D12-2 defect anchored it to the head's end, so
    the mid-head anaphora "They themselves <universal>" waived)."""
    if _SENTENCE_MENTION_RE.search(sentence) is not None:
        return False
    head = sentence[:position].strip()
    if not head:
        return False
    if _PRONOUN_SUBJECT_RE.search(head):
        return False
    return not _POPULATION_SUBJECT_RE.search(head)


def _unit_scoped(scope_text: str) -> bool:
    """Whether ANY sentence of the unit carries a genuine scope marker
    (rules 1/3/4's reach: their claims — a family definition, a
    parity/mirror claim, a typed declaration — bind to the unit's own
    scope statement)."""
    return any(_sentence_scoped(sentence) for sentence in _sentences(scope_text))


def _claim_unwaived(
    scope_text: str,
    pattern: re.Pattern[str],
    *,
    subject_waiver: bool = False,
    sentence_local: bool = False,
) -> bool:
    """Whether the claim pattern has at least one occurrence its own
    text does not waive. Unit-level claims (rules 1/3) are waived by a
    genuine scope marker anywhere in the unit (`_unit_scoped`); a
    sentence-local claim (rule 2's universal gloss) is waived only
    within the occurrence's OWN sentence (`_sentence_scoped`), or when
    the occurrence is one reason's own true meaning: its subject is
    that reason's own rows with no population subject anywhere in the
    sentence before it (`_subject_scoped`), it is bound to one reason's
    label — no population subject between label and occurrence, and a
    population subject anywhere earlier defeats it unless the
    occurrence sits inside the last label's own unclosed parenthetical
    (`_label_bound`), or its own sentence is a foreign restricted
    subject (`_foreign_subject`)."""
    unit_scope = None if sentence_local else _unit_scoped(scope_text)
    if unit_scope:
        return False
    for sentence in _sentences(scope_text):
        claim_spans = [match.span() for match in pattern.finditer(sentence)]
        sentence_scope = _sentence_scoped(sentence, claim_spans) if sentence_local else False
        for match in pattern.finditer(sentence):
            if sentence_scope:
                continue
            if subject_waiver and _subject_scoped(sentence, match.start()):
                continue
            if sentence_local and _label_bound(sentence, match.start()):
                continue
            if sentence_local and _foreign_subject(sentence, match.start()):
                continue
            return True
    return False


def rule_closed_enumeration(unit: Unit) -> bool:
    """Rule 1: the unit DEFINES the stranded reason family but supplies
    only the old dispatch conditions — the two-label closed set, the
    prose two-member form, OR the single-member definition (the
    settings-description class: defining the entire check by no-config
    alone is the same defect; the rule never requires the word 'two').
    A corrected unit either includes `workflow_root_missing` and its
    distinct meaning, or its defining claim is expressly scoped (a
    genuine scope statement in the unit, or the defining claim's own
    subject is one reason's rows)."""
    if not _unit_mentions_detector(unit):
        return False
    lowered = _lowered(unit)
    if "workflow_root_missing" in lowered:
        return False
    mentions_old = (
        "no_actor_config" in lowered
        or "no actor_config" in lowered
        or "unserved_queue" in lowered
        or "unserved" in lowered
        or "no live worker" in lowered
    )
    if not mentions_old:
        return False
    return _claim_unwaived(_scope_text(unit), _DEFINING_RE, subject_waiver=True)


def rule_universal_no_dispatch(unit: Unit) -> bool:
    """Rule 2: a universal no-dispatch/no-touch/invisible claim attached
    to the stranded detector/gauge that its own occurrence does not
    waive — the occurrence's own sentence must carry a genuine scope
    marker (`_sentence_scoped`), or the occurrence must be one reason's
    own true meaning (its subject is that reason's own rows with no
    population subject in the sentence before it, it is bound to one
    reason's label with no population subject defeating it — the
    label's own unclosed parenthetical excepted —, or its sentence is a
    foreign restricted subject)."""
    if not _unit_mentions_detector(unit):
        return False
    return _claim_unwaived(
        _scope_text(unit), _UNIVERSAL_RE, subject_waiver=True, sentence_local=True
    )


def rule_whole_detector_parity(unit: Unit) -> bool:
    """Rule 3: a whole-detector parity/mirror claim that the unit does
    not narrow to the two dispatch-invisible legs anywhere in its own
    text (a parity claim binds to the unit's scope statement)."""
    if not _unit_mentions_detector(unit):
        return False
    return _claim_unwaived(_scope_text(unit), _PARITY_RE)


def rule_structural_closed_enumeration(unit: Unit) -> bool:
    """Rule 4: a `Literal[...]`/tuple/list/set DECLARATION of the reason
    family lacking `workflow_root_missing` — the typed face of the closed
    enumeration. A genuine subset fixture (a test's own old-label tuple)
    passes only when the span is allowlisted or the same file carries the
    third-reason positive coverage."""
    if not _unit_mentions_detector(unit):
        return False
    text = unit.text
    if "workflow_root_missing" in text:
        return False
    has_labels = "no_actor_config" in text and "unserved_queue" in text
    if not has_labels:
        return False
    # The declaration is one statement; its scope is any genuine scope
    # sentence the unit carries.
    return not _unit_scoped(_scope_text(unit))


RULES = (
    ("closed-enumeration", rule_closed_enumeration),
    ("universal-no-dispatch", rule_universal_no_dispatch),
    ("whole-detector-parity", rule_whole_detector_parity),
    ("structural-closed-enumeration", rule_structural_closed_enumeration),
)


# ── the scan ────────────────────────────────────────────────────────────


def _iter_scan_files(base: Path) -> Iterator[Path]:
    for root in SCAN_ROOTS:
        base_dir = base / root
        for path in sorted(base_dir.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix not in SCAN_SUFFIXES:
                continue
            yield path


def _units_for(rel: str, raw: str) -> list[Unit]:
    """The file's semantic units (the scan's own dispatch, shared with
    the anti-rot law so a digest's live-span match is computed over the
    SAME units the scan matches). Each surface owns its text with its
    real grammar's parser: a `.py` file the AST, a `.yml`/`.yaml` file
    the YAML composer's node marks (scalar keys and values, comment
    lines, plus the per-line floor), an `.html` file the element/comment
    grammar plus a no-fold per-line floor for the template text no
    element or comment owns, a `.md` file the Markdown block parser's
    token/line map plus the MkDocs container bodies and the per-line
    floor. The old markdown floors on `.html`/`.yaml` are gone: template
    text is not markdown, and the floor's paragraphs folded non-blank
    template text into one unit — the laundering shape this model exists
    to prevent. The floor itself is now every text-carrier surface's OWN
    gap coverage — per line, never folded — so the line-coverage law
    (`_coverage_violations`) holds by construction and is checked.
    Fail-closed: a .py file that does not parse raises, unreadable YAML
    raises, an unparseable template raises, and a Markdown block token
    the unit model does not know raises — never a silent skip."""
    if rel.endswith(".py"):
        tree = ast.parse(raw)
        return list(_python_units(rel, raw, tree))
    if rel.endswith((".yml", ".yaml")):
        units = list(_yaml_scalar_units(rel, raw)) + list(_yaml_comment_units(rel, raw))
        units.extend(_line_floor_units(rel, raw, units, kind="yaml-text"))
        return units
    if rel.endswith(".html"):
        return list(_html_units(rel, raw))
    return list(_markdown_units(rel, raw))


def _line_floor_units(rel: str, raw: str, units: list[Unit], kind: str) -> list[Unit]:
    """The per-line FLOOR: every non-blank line of the text no existing
    unit owns is its own unit, never folded — the mechanism that makes
    the line-coverage law true for a text-carrier surface (the .md
    link-reference definitions the parser consumes without a token and
    the `>`-only separators were outside every unit — the round-9 D9-8
    defeat; the .yaml folded-scalar lines were outside every unit —
    D9-7). An extra unit can only add a red, never hide one."""
    lines = _split_lines(raw)
    covered: set[int] = set()
    for unit in units:
        covered.update(range(unit.start_line, unit.end_line + 1))
    return [
        Unit(rel, kind, number, number, line.strip())
        for number, line in enumerate(lines, 1)
        if number not in covered and line.strip()
    ]


def _allowlisted(path: str, unit: Unit) -> bool:
    for digest, allowed_path, _reason in ALLOWLIST:
        if path != allowed_path:
            continue
        if digest == "*":
            return True
        if digest == unit.span_digest:
            return True
    return False


def _scan(base: Path | None = None) -> list[tuple[str, Unit, str]]:
    """The full scan: every unit x every rule; the unlisted matches are
    the failures. Fail-closed on unreadable files and parse errors.
    `base` relocates the scan to a caller-supplied corpus root (the
    fail-closed control's runtime fixtures — nothing under the real
    docs/, src/ or tests/ is ever created); the default scans the tree."""
    root = REPO_ROOT if base is None else base
    failures: list[tuple[str, Unit, str]] = []
    for path in _iter_scan_files(root):
        rel = str(path.relative_to(root))
        try:
            raw = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError) as exc:
            pytest.fail(f"{rel}: unreadable file — the scan fails closed: {exc}")
        try:
            units = _units_for(rel, raw)
        except SyntaxError as exc:
            pytest.fail(f"{rel}: ast.parse failed — the scan fails closed: {exc}")
        for unit in units:
            for rule_name, rule in RULES:
                if rule(unit):
                    if _allowlisted(rel, unit):
                        continue
                    failures.append((rule_name, unit, rel))
    return failures


# ── the unit model's structural laws ────────────────────────────────────
#
# Round 9's lesson: a unit model maintained by PER-CONSTRUCT enumeration
# is always one construct short (nine rounds of defeats). These laws are
# the model's checked properties — asserted over EVERY scanned file, so
# a construct no round thought of cannot leave its text OUTSIDE the
# units (what a unit's own interior waives is the scoping predicate's
# declared business: the module docstring's residuals). Each law
# returns human-readable violations; `test_unit_model_laws_hold_...`
# asserts them empty over the tree and `test_unit_model_laws_red_...`
# proves each law reds on an injected violation.


def _coverage_violations(rel: str, raw: str, units: list[Unit]) -> list[str]:
    """LAW 1 — line coverage: every non-blank line of a text-carrier
    file (`.md`, `.yml`, `.yaml`, `.html`) belongs to at least one unit.
    For a `.py` file the law is the OWNERSHIP form
    (`_py_text_ownership_violations`): a Python file's only text
    carriers are its string literals and comments (code tokens carry no
    prose for these rules to match), so the law pins every
    tokenize-level STRING/COMMENT token to a unit instead of pinning
    code lines. The per-line floors are what make the text-carrier form
    true; this law is what makes losing a floor red."""
    if rel.endswith(".py"):
        return _py_text_ownership_violations(rel, raw, units)
    lines = _split_lines(raw)
    covered: set[int] = set()
    for unit in units:
        covered.update(range(unit.start_line, unit.end_line + 1))
    return [
        f"{rel}:{number}: non-blank line outside every unit: {lines[number - 1][:80]!r}"
        for number, line in enumerate(lines, 1)
        if line.strip() and number not in covered
    ]


def _py_text_ownership_violations(rel: str, raw: str, units: list[Unit]) -> list[str]:
    """LAW 1's Python face: every tokenize-level STRING and COMMENT
    token's span is owned by at least one unit (the AST surfaces are the
    closed text-carrier set of a Python file)."""
    import io
    import tokenize

    # Units sorted by start with a running max end: a token span
    # (start <= end) is owned iff some unit starting at or before it
    # reaches at least its end — the bisected running maximum answers
    # that in O(log n).
    ordered = sorted((u.start_line, u.end_line) for u in units)
    starts = [start for start, _end in ordered]
    running_max: list[int] = []
    best = 0
    for _start, end in ordered:
        best = max(best, end)
        running_max.append(best)

    def owned(start: int, end: int) -> bool:
        index = bisect.bisect_right(starts, start)
        return index > 0 and running_max[index - 1] >= end

    violations: list[str] = []
    for token in tokenize.generate_tokens(io.StringIO(raw).readline):
        if token.type not in (tokenize.STRING, tokenize.COMMENT):
            continue
        if not owned(token.start[0], token.end[0]):
            violations.append(
                f"{rel}:{token.start[0]}-{token.end[0]}: a "
                f"{tokenize.tok_name[token.type]} token is outside every unit"
            )
    return violations


def _overlap_violations(rel: str, units: list[Unit]) -> list[str]:
    """LAW 2 — laminar: every PAIR of a file's units is either disjoint,
    one contains the other, or they TOUCH at a shared boundary line (an
    element own-run ends mid-line where the next run begins — the
    .html/.md element model's own granularity; a touch is not a
    crossing). This is the all-pairs property, and it is decided by the
    standard open-interval STACK over units sorted by (start, longest
    first): when a unit begins, every unit still open began at or
    before it, so if the new unit ends beyond the innermost open unit
    it crosses THAT unit — a pair the running-maximum sweep never
    compared (the round-10 F10-1 defect: two units both contained in a
    third crossed each other and the sweep reported the family clean;
    the round-8 adjacent check caught one such shape and the sweep
    caught none of the three). Popping is amortized O(n) over the
    O(n log n) sort, so the check stays linearithmic in each file's
    unit count; the corpus test asserts agreement with the brute-force
    all-pairs form on randomized inputs, including nested-crossing
    shapes."""
    ordered = sorted(units, key=lambda unit: (unit.start_line, -unit.end_line))
    violations: list[str] = []
    stack: list[tuple[int, int]] = []  # the open units, innermost last
    for unit in ordered:
        start, end = unit.start_line, unit.end_line
        while stack and stack[-1][1] <= start:
            # Closed before this unit begins (a shared boundary line is
            # the touch case — not a crossing).
            stack.pop()
        if stack and end > stack[-1][1]:
            owner = f"{stack[-1][0]}-{stack[-1][1]}"
            violations.append(
                f"{rel}: {owner} partially overlaps {start}-{end} — a unit crossing a "
                "construct boundary"
            )
        stack.append((start, end))
    return violations


_TRUNCATION_SUBSTRING_KINDS: Final = frozenset(
    {"html-element", "html-comment", "template-comment", "collection", "literal-type", "comment"}
)


def _truncation_violations(rel: str, raw: str, units: list[Unit]) -> list[str]:
    """LAW 3 — no truncation: a unit's text is consistent with its
    declared line span (the round-9 D9-5 defeat: splitting the file on
    `str.splitlines`' extra characters truncated a unit's text and hid
    text outside every unit). Whole-line units carry EXACTLY the file's
    own raw lines over their span; statement units carry an exact
    substring of the file; Python string/docstring units carry PARSED
    values (escapes resolved — their value may hold more lines than the
    source span), so their law is the span's existence plus the
    ownership law. The markdown surface's reconstruction runs on the
    renderer's own tab-expanded grid — the grid the model built the
    units from."""
    lines = _renderer_lines(raw) if rel.endswith(".md") else _split_lines(raw)
    violations: list[str] = []
    for unit in units:
        if unit.start_line < 1 or unit.end_line > len(lines) or unit.start_line > unit.end_line:
            violations.append(
                f"{rel}:{unit.start_line}-{unit.end_line}: span outside the "
                f"file's {len(lines)} lines"
            )
            continue
        span_text = "\n".join(lines[unit.start_line - 1 : unit.end_line])
        if unit.kind in _TRUNCATION_SUBSTRING_KINDS:
            # A statement unit's text is an exact substring of its own
            # span's text — reconstructed from the file's raw lines minus
            # the unit's container dedent (a container body's units carry
            # the dedented text the renderer parses).
            span_dedented = "\n".join(
                "" if not ln.strip() else ln[getattr(unit, "dedent", 0) :]
                for ln in lines[unit.start_line - 1 : unit.end_line]
            )
            if unit.text not in span_dedented:
                violations.append(
                    f"{rel}:{unit.start_line}-{unit.end_line}: a {unit.kind} "
                    "unit's text is not a substring of its own span"
                )
            continue
        if unit.kind == "yaml":
            # The parsed (folded) value must OCCUR in its own span: the
            # value's first non-empty line is one of the span's lines,
            # stripped.
            first = next((ln for ln in unit.text.splitlines() if ln.strip()), "")
            if first.strip() not in " ".join(
                ln.strip() for ln in lines[unit.start_line - 1 : unit.end_line]
            ):
                violations.append(
                    f"{rel}:{unit.start_line}-{unit.end_line}: a yaml unit's "
                    f"parsed value does not occur in its span: {unit.text[:80]!r}"
                )
            continue
        if rel.endswith(".py"):
            # A parsed string value may hold more lines than its source
            # span (escape sequences); the span's existence is the law
            # here (checked above), ownership is law 1.
            continue
        if unit.kind == "markdown":
            expected_lines = [
                "" if not ln.strip() else ln[getattr(unit, "dedent", 0) :]
                for ln in lines[unit.start_line - 1 : unit.end_line]
            ]
            expected = "\n".join(expected_lines)
            truncated = (
                unit.text != expected
                if unit.end_line > unit.start_line
                else unit.text not in (expected, expected.strip())
            )
            if truncated:
                violations.append(
                    f"{rel}:{unit.start_line}-{unit.end_line}: a markdown "
                    "unit's text is not exactly the file's own raw lines minus "
                    f"its container dedent ({getattr(unit, 'dedent', 0)})"
                )
            continue
        if unit.end_line > unit.start_line:
            if unit.text != span_text:
                violations.append(
                    f"{rel}:{unit.start_line}-{unit.end_line}: a multi-line "
                    "unit's text is not exactly the file's own raw lines"
                )
        elif unit.text not in (span_text, span_text.strip()):
            violations.append(
                f"{rel}:{unit.start_line}: a single-line unit's text is not its own line"
            )
    return violations


# ── the invariant itself ────────────────────────────────────────────────


def test_stranded_reason_vocabulary_tree_wide_invariant() -> None:
    """The tree-wide invariant: no closed reason-set enumeration, no
    universal no-dispatch gloss, no whole-detector parity claim, and no
    typed closed enumeration outside the span-digest allowlist. An
    unlisted match is red; a vanished allowlisted span is red."""
    # The allowlist's own anti-rot law: every non-wildcard entry's path
    # must EXIST (a vanished span is red).
    failures = _scan()
    assert not failures, (
        "the stranded-reason vocabulary invariant found uncorrected surfaces "
        "(every entry needs its own correction or an allowlist entry with a "
        "one-line reason):\n"
        + "\n".join(
            f"  [{rule}] {path}:{unit.start_line}-{unit.end_line} ({unit.kind})\n"
            f"    {unit.text[:240]!r}"
            for rule, unit, path in failures
        )
    )


def test_required_vocabulary_homes_carry_the_third_reason() -> None:
    """The POSITIVE-PRESENCE pin (the per-site red law's mechanical
    half): every home the correction names must CARRY the third reason's
    spellings. Reverting/deleting any home's own wording reds HERE with
    the path identified — including the pure-addition homes (the
    mechanism documentation) whose reverts restore no prose LIE for the
    matcher rules but lose the vocabulary itself."""
    for path, required, why in _REQUIRED_HOMES:
        full = REPO_ROOT / path
        assert full.exists(), f"{path} (a required vocabulary home) vanished"
        text = full.read_text(encoding="utf-8")
        for token in required:
            assert token in text, (
                f"the required vocabulary home {path} lost its '{token}' "
                f"spelling ({why}) — reverting a corrected site is red"
            )


#: The class's own vocabulary: a WHOLE-FILE (wildcard) exception exists
#: only while its file still carries the class's words — an emptied
#: exception is a stale one (the round-4 NEW-5 defeat: DeepSeek emptied
#: the frozen oracle and the invariant passed both). The spellings are
#: the class's own surfaces: the snake_case reason labels, the hyphenated
#: release/log spellings, the typed corruption class, and the family's
#: own name.
_WILDCARD_VOCABULARY: Final[tuple[str, ...]] = (
    "no_actor_config",
    "unserved_queue",
    "workflow_root_missing",
    "workflow-root-missing",
    "WorkflowRootMissingError",
    "stranded",
)


def _allowlist_rotations(
    entries: tuple[tuple[str, str, str], ...],
    *,
    file_text: Callable[[str], str | None],
) -> list[str]:
    """The allowlist's anti-rot law (round-4 NEW-5), driveable on any
    (entries, reader) pair so the defeats are runtime-constructed
    fixtures and never tree mutations. Every entry must remain JUSTIFIED
    against the LIVE text:

    * the path exists (a missing path is a violation);
    * a span-digest entry's digest matches a LIVE unit of the file's
      CURRENT text (an edited or deleted span is a violation — deleting
      or rewording text cannot silently erase an exception);
    * that live unit still FIRES at least one rule (round-12 D12-5): a
      live-but-not-firing entry is a STALE SUPPRESSION — it hides
      nothing today but silently suppresses a future widened rule on
      the same span; prune the entry or re-justify the wording;
    * a wildcard entry's file still CARRIES the class's vocabulary (an
      emptied exception is a stale one).

    Returns one human-readable violation per defect; empty = every
    entry is live and firing."""
    violations: list[str] = []
    for digest, allowed_path, reason in entries:
        if not reason or len(reason.splitlines()) != 1:
            violations.append(f"{allowed_path}: the entry's reason is not one line: {reason!r}")
            continue
        raw = file_text(allowed_path)
        if raw is None:
            violations.append(
                f"{allowed_path}: the allowlisted path vanished ({digest}) — deleting "
                "text cannot silently erase an exception; prune or re-justify the entry"
            )
            continue
        if digest == "*":
            if not any(word in raw for word in _WILDCARD_VOCABULARY):
                violations.append(
                    f"{allowed_path}: the whole-file exception is STALE — the file "
                    "no longer carries the class's vocabulary; prune the entry"
                )
            continue
        try:
            units = _units_for(allowed_path, raw)
        except (SyntaxError, AssertionError) as exc:
            violations.append(f"{allowed_path}: the file does not parse (fail-closed): {exc}")
            continue
        live = [unit for unit in units if unit.span_digest == digest]
        if not live:
            violations.append(
                f"{allowed_path}: the allowlisted span's digest matches NO live unit "
                f"({digest}) — the span was edited or deleted; re-derive or prune "
                "the entry"
            )
            continue
        if not any(rule(unit) for unit in live for _name, rule in RULES):
            violations.append(
                f"{allowed_path}: the allowlisted span's LIVE unit fires NO rule "
                f"({digest}) — the exception suppresses nothing; a stale suppression "
                "waiting to hide a widened rule; prune the entry or re-justify "
                "the wording"
            )
    return violations


def test_allowlist_defeats_red_the_anti_rot_law() -> None:
    """Round-4 NEW-5's defeats, runtime-constructed (the tree is never
    touched): (a) a NEUTRALISED SPAN — an allowlisted digest whose span's
    text was edited no longer matches a live unit and reds, and a
    vanished PATH reds; (b) an EMPTIED WHOLE-FILE exception — the
    wildcard entry whose file lost the class's vocabulary reds. Each
    defeat's positive (the same entries against the live text) is
    asserted clean, so the reds are the law's own and not a broken
    fixture."""
    # A violating paragraph the way the shipped tree carries one: the
    # unit's digest is computed by the REAL Unit over the REAL unit
    # builder's output. The violating text is assembled at runtime from
    # independently non-triggering fragments (the module docstring's own
    # law) — no stored constant carries the full legacy combination.
    live_text = (
        "# A guide\n\n"
        "The stranded detector's reasons are "
        + frag_reasons
        + " and "
        + frag_unserved
        + " — that is the "
        + "whole set.\n"
    )
    units = list(_markdown_units("virtual/guide.md", live_text))
    # The heading and the paragraph are separate units; the SANCTIONED
    # span is the paragraph that carries the match.
    assert len(units) == 2, [u.text for u in units]
    violating = next(u for u in units if "stranded" in u.text)
    span_digest = violating.span_digest

    entries: tuple[tuple[str, str, str], ...] = (
        (span_digest, "virtual/guide.md", "the fixture's own sanctioned span"),
        ("*", "virtual/oracle.md", "the fixture's own whole-file exception"),
    )

    def _live_reader(rel: str) -> str | None:
        return {
            "virtual/guide.md": live_text,
            "virtual/oracle.md": (
                "the oracle's own text mentions " + "stranded rows: " + frag_reasons
            ),
        }.get(rel)

    # THE POSITIVE: both entries are live against the un-edited text.
    assert _allowlist_rotations(entries, file_text=_live_reader) == []

    # DEFEAT (a1): the span NEUTRALISED — the guide's paragraph is
    # reworded; the digest matches no live unit and the entry reds.
    def _edited_reader(rel: str) -> str | None:
        if rel == "virtual/guide.md":
            return live_text.replace(
                "that is the whole set.", "is scoped to the two dispatch-invisible legs."
            )
        return _live_reader(rel)

    violations = _allowlist_rotations(entries, file_text=_edited_reader)
    assert any("matches NO live unit" in v and "virtual/guide.md" in v for v in violations), (
        violations
    )

    # DEFEAT (a2): the span VANISHED with its path — the reader answers
    # None and the entry reds.
    violations = _allowlist_rotations(
        entries, file_text=lambda rel: _live_reader(rel) if rel != "virtual/guide.md" else None
    )
    assert any("path vanished" in v and "virtual/guide.md" in v for v in violations), violations

    # DEFEAT (b): the WHOLE-FILE exception EMPTIED — the oracle loses the
    # class's vocabulary and the wildcard entry reds.
    violations = _allowlist_rotations(
        entries,
        file_text=lambda rel: (
            _live_reader(rel) if rel != "virtual/oracle.md" else "an unrelated file's text"
        ),
    )
    assert any("STALE" in v and "virtual/oracle.md" in v for v in violations), violations

    # DEFEAT (c) — the round-12 D12-5 law: a LIVE-BUT-NOT-FIRING span.
    # The entry's digest matches a live unit, but no rule fires on it
    # any more (the wording was corrected without pruning the
    # exception) — a stale suppression, and a violation of the
    # strengthened law. The positive above doubles as this conjunct's
    # clean control: the guide paragraph fires closed-enumeration, so
    # `== []` already asserts the firing conjunct holds for a justified
    # entry.
    scoped_text = (
        "# A guide\n\nThe stranded detector is scoped to the two dispatch-invisible legs.\n"
    )
    scoped_units = list(_markdown_units("virtual/scoped.md", scoped_text))
    scoped_unit = next(u for u in scoped_units if "stranded" in u.text)
    assert not any(rule(scoped_unit) for _n, rule in RULES), (
        "the defeat fixture's span must be live but NOT fire (the stale-suppression shape)"
    )
    scoped_entries: tuple[tuple[str, str, str], ...] = (
        (scoped_unit.span_digest, "virtual/scoped.md", "the fixture's stale exception"),
    )
    violations = _allowlist_rotations(
        scoped_entries, file_text=lambda rel: scoped_text if rel == "virtual/scoped.md" else None
    )
    assert any("fires NO rule" in v and "virtual/scoped.md" in v for v in violations), violations


def test_allowlist_paths_exist_and_spans_are_justified() -> None:
    """The allowlist's own anti-rot law, driven against the LIVE tree:
    every entry's path exists; every span-digest entry's digest matches a
    LIVE unit of the current text (an edited or deleted span is red); a
    wildcard entry's file still carries the class's vocabulary (an empty
    exception is a stale one); every entry carries a one-line reason.
    The defeats are runtime-constructed fixtures below — they prove THIS
    helper reds, never a re-implementation."""
    violations = _allowlist_rotations(
        ALLOWLIST,
        file_text=lambda rel: (
            (REPO_ROOT / rel).read_text(encoding="utf-8") if (REPO_ROOT / rel).exists() else None
        ),
    )
    assert not violations, "the allowlist's anti-rot law found rotated entries:\n" + "\n".join(
        f"  - {v}" for v in violations
    )
    # The frozen oracle's existence is load-bearing (the differential
    # gate's own reference).
    assert (REPO_ROOT / "tests/_audit_vendor/_old_leader_sweeps.py").exists()


# ── the runtime-constructed self-referential fixtures ───────────────────


#: The corpus's own label fragments — each piece independently
#: non-triggering (the module docstring's runtime-construction law: no
#: stored constant carries a marker-and-rule combination, so the
#: scanner's own fixtures can never exempt or poison the scan).
frag_reasons = "no_actor_" + "config"
frag_unserved = "unserved_" + "queue"
frag_universal = "permanently " + "undispatchable"
frag_forever = "never " + "dispatched"


def _corpus_units(virtual_path: str, text: str) -> list[Unit]:
    """The scanner's own unit builder, callable on a caller-supplied
    corpus (the fixtures' shape) — nothing under docs/ or src/ is ever
    created, edited, or removed. It IS the scan's own dispatch
    (`_units_for`), so a fixture exercises the same unit model the
    tree-wide scan uses — never a shadow re-implementation."""
    return _units_for(virtual_path, text)


def _corpus_matches(virtual_path: str, text: str) -> list[str]:
    hits = []
    for unit in _corpus_units(virtual_path, text):
        for rule_name, rule in RULES:
            if rule(unit):
                hits.append(rule_name)
    return hits


def test_negative_corpus_unscoped_variants_red() -> None:
    """The in-memory negative corpus: every legacy unscoped form reds —
    the two-member closed enumeration (labels and prose), the single-
    member definition (the settings-description class), the universal
    gloss, the unscoped parity claim, and the typed closed enumeration.
    The fragments are constructed at runtime from independently
    non-triggering pieces."""
    # Independently non-triggering fragments (never a stored
    # counterexample). Every legacy corpus string below is assembled at
    # runtime from pieces none of which carries a marker-and-rule
    # combination — the module docstring's own runtime-construction law:
    # the scanner's stored constants must never be their own counter-
    # examples (the pre-fix literals were, hidden by the file's own
    # wildcard — the round-5 F-1 removal).
    frag_actor = "the " + "actor has no " + "actor_config row, so"
    frag_queue = " the queue has " + "no live worker"
    frag_dispatch = "can never be " + "dispatched"
    legacy_two_labels = (
        "The stranded detector's reasons are "
        + frag_reasons
        + " and "
        + frag_unserved
        + " — that is the "
        + "whole set."
    )
    legacy_prose = (
        "Pending/scheduled jobs that can never "
        + "dispatch: "
        + frag_actor
        + " dispatch never sees the rows;"
        + frag_queue
        + " subscribed. Neither dispatch nor the deadline sweep will ever touch these rows."
    )
    legacy_single = (
        "The stranded-jobs check runs for pending jobs "
        + "whose actor has no "
        + "actor_config row."
    )
    legacy_typed = (
        "type StrandedReason = Literal['"
        + frag_reasons
        + "', '"
        + frag_unserved
        + "'] "
        + "# the closed reason set of the stranded "
        + "detector"
    )
    # The unscoped PARITY claim (the doctor/queue-roll-up class): the
    # same non-triggering fragment discipline — the rule-3 trigger is
    # itself assembled from pieces, so no stored constant carries it.
    frag_parity = "same " + "computation"
    legacy_parity = (
        "The doctor's queue roll-up asks the stranded gauge the "
        + frag_parity
        + " the leader tick asks, unscoped."
    )
    for name, text in (
        ("legacy-two-labels", legacy_two_labels),
        ("legacy-prose", legacy_prose),
        ("legacy-single", legacy_single),
        ("legacy-typed", legacy_typed),
        ("universal-gloss", f"The stranded gauge: rows that {frag_dispatch}."),
        ("unscoped-parity", legacy_parity),
    ):
        hits = _corpus_matches(f"virtual/{name}.md", text)
        assert hits, f"the negative corpus's {name} must red the scanner"

    # The typed closed enumeration's PYTHON face (assembled from the same
    # non-triggering fragments).
    py_negative = (
        "from typing import Literal\n"
        "type StrandedReason = Literal['" + frag_reasons + "', '" + frag_unserved + "']\n"
        '"""The stranded detector\'s own closed reason set."""\n'
    )
    assert _corpus_matches("virtual/negative.py", py_negative)
    # The assembled values are the EXACT legacy strings the class is
    # about — the fragmentation may never dilute the corpus. The pin is
    # each value's own rule signature (which rules fire, in RULES order)
    # — exact, yet storing no legacy combination as a constant.
    assert _corpus_matches("virtual/two.md", legacy_two_labels) == [
        "closed-enumeration",
        "structural-closed-enumeration",
    ]
    assert _corpus_matches("virtual/prose.md", legacy_prose) == [
        "closed-enumeration",
        "universal-no-dispatch",
    ]
    assert _corpus_matches("virtual/single.md", legacy_single) == ["closed-enumeration"]
    assert _corpus_matches("virtual/typed.md", legacy_typed) == [
        "closed-enumeration",
        "structural-closed-enumeration",
    ]
    assert _corpus_matches("virtual/negative.py", py_negative) == [
        "structural-closed-enumeration",
        "structural-closed-enumeration",
    ]
    # The parity value's own exact signature: the `whole-detector-parity`
    # rule family is exercised by the corpus — deleting the rule family
    # reds HERE (it can no longer be deleted without a red).
    assert _corpus_matches("virtual/parity.md", legacy_parity) == ["whole-detector-parity"]


def test_positive_corpus_all_three_and_scoped_forms_pass() -> None:
    """The in-memory positive corpus: the corrected forms pass — the
    all-three meanings, the express dispatch-invisible subset scoping,
    the scoped parity claims, and the subject-scoped non-defects (the
    deadline/bootstrap text without detector context)."""
    all_three = (
        "The stranded detector's reason set: no_actor_config (the row is "
        "invisible to every dispatch round — no dispatch or deadline touch), "
        "unserved_queue (likewise dispatch-invisible), and "
        "workflow_root_missing (the row may dispatch and run when served, "
        "but the run cannot progress — control-plane corruption, the "
        "runbook's remedy)."
    )
    scoped_subset = (
        "This read mirrors only the two dispatch-invisible legs (no_actor_"
        "config and unserved_queue); the detector's run-scoped "
        "workflow_root_missing leg is not mirrored here."
    )
    scoped_parity = (
        "The two stranded-jobs families are the same computation for the "
        "two dispatch-invisible legs only — the third leg is the leader "
        "tick's own."
    )
    deadline_subject = (
        "The deadline sweep fails jobs past their schedule_to_close: a job "
        "the deadline fails is dispatched before it dies."
    )
    for name, text in (
        ("all-three", all_three),
        ("scoped-subset", scoped_subset),
        ("scoped-parity", scoped_parity),
        ("deadline-subject", deadline_subject),
    ):
        hits = _corpus_matches(f"virtual/{name}.md", text)
        assert not hits, f"the positive corpus's {name} must pass: {hits}"

    # The typed all-three face passes structurally.
    py_positive = (
        "from typing import Literal\n"
        "type StrandedReason = Literal['no_actor_config', 'unserved_queue', "
        "'workflow_root_missing']\n"
        '"""The stranded detector\'s own closed reason set."""\n'
    )
    assert not _corpus_matches("virtual/positive.py", py_positive)


def test_parser_fails_closed_on_unreadable_and_unparseable(tmp_path: Path) -> None:
    """The fail-closed disposition itself cannot regress, driven through
    the REAL scanner entry point (`_scan`, the same function the
    tree-wide invariant calls) over a runtime corpus — nothing under the
    real docs/, src/ or tests/ is ever created. A file that cannot be
    read or parsed is RED with the path NAMED, never skipped; a clean
    corpus scans empty (the positive control, so the reds are the
    fail-closed law's own and not a broken fixture)."""
    # The cheap face first: the broken module's syntax error is real.
    broken = "def broken(:\n    pass\n"
    with pytest.raises(SyntaxError):
        ast.parse(broken)
    for root in SCAN_ROOTS:
        (tmp_path / root).mkdir()
    (tmp_path / "src" / "clean.py").write_text("x = 1\n", encoding="utf-8")
    assert _scan(tmp_path) == [], "a clean corpus must scan empty"

    # (a) an unparseable .py: the scan fails closed, naming the path.
    (tmp_path / "src" / "clean.py").unlink()
    (tmp_path / "src" / "broken.py").write_text(broken, encoding="utf-8")
    with pytest.raises(pytest.fail.Exception) as unparseable:
        _scan(tmp_path)
    assert "src/broken.py" in str(unparseable.value)
    assert "fails closed" in str(unparseable.value)
    (tmp_path / "src" / "broken.py").unlink()

    # (b) an unreadable YAML: the YAML dispatch fails closed, naming the
    # path (the AssertionError surfaces through the scan).
    (tmp_path / "src" / "broken.yaml").write_text("alert: [unclosed\n", encoding="utf-8")
    with pytest.raises(AssertionError) as unreadable_yaml:
        _scan(tmp_path)
    assert "src/broken.yaml" in str(unreadable_yaml.value)
    (tmp_path / "src" / "broken.yaml").unlink()

    # (c) an undecodable file: the read fails closed, naming the path.
    (tmp_path / "docs" / "binary.md").write_bytes(b"\xff\xfe\x00broken")
    with pytest.raises(pytest.fail.Exception) as unreadable:
        _scan(tmp_path)
    assert "docs/binary.md" in str(unreadable.value)
    assert "unreadable file" in str(unreadable.value)


def test_new_occurrence_reds_without_review() -> None:
    """The consequence (the layer-7 law): a NEW occurrence anywhere in the
    scanned tree produces an unlisted matcher result and reds this
    invariant — the corpus shape proves the mechanism end to end (the
    same text passes when corrected, reds when the old wording returns)."""
    occurrence = (
        "The stranded detector: "
        + "pending/scheduled jobs that "
        + "can never be dispatched ("
        + frag_reasons
        + ", "
        + frag_unserved
        + ")."
    )
    assert _corpus_matches("virtual/regression.md", occurrence), (
        "the legacy wording must red the scanner the moment it returns"
    )
    occurrence_tail = "can never be dispatched (" + frag_reasons + ", " + frag_unserved + ")."
    corrected = occurrence.replace(
        occurrence_tail,
        "cannot make progress (the two dispatch-invisible reasons, plus "
        "workflow_root_missing's run-scoped meaning).",
    )
    assert not _corpus_matches("virtual/regression.md", corrected)
    # The assembled value is the EXACT legacy wording, pinned by its own
    # rule signature (no legacy combination stored as a constant).
    assert _corpus_matches("virtual/regression.md", occurrence) == [
        "closed-enumeration",
        "universal-no-dispatch",
        "structural-closed-enumeration",
    ]


def test_table_rows_are_their_own_units_a_laundered_gloss_reds() -> None:
    """Round-4 NEW-4: the invariant launders a gloss inside a Markdown
    table when the unit splitter treats the WHOLE table as one unit —
    DeepSeek inserted a two-member/universal-gloss row into a table whose
    SIBLING row carried the scoping marker and the invariant stayed
    green. Each row is its own unit now: the gloss row reds ALONE, the
    marker row never purifies it, and the fully-corrected table
    passes."""
    header = "| Reason | Meaning |"
    separator = "| --- | --- |"
    # The ATTACK: one corrected (scoped) row + one universal-gloss row
    # (the gloss row assembled from non-triggering fragments — the
    # module's own runtime-construction law).
    gloss_row = "| " + frag_reasons + " | these rows " + "can never " + "dispatch |"
    marker_row = (
        "| workflow_root_missing | the run cannot progress because its "
        "__flow__ root is gone (the dispatch-invisible rows are a separate "
        "class) |"
    )
    attacked_table = "\n".join((header, separator, marker_row, gloss_row))
    assert _corpus_matches("virtual/table-gloss.md", attacked_table), (
        "the gloss row must red the scanner EVEN WHEN a sibling row "
        "carries the scoping marker — a table row is its own unit"
    )
    # The gloss row reds ALONE (the row's own unit, no sibling context).
    assert _corpus_matches("virtual/row-alone.md", gloss_row)
    # The fully-corrected table passes (every row's own meaning is clean).
    corrected_gloss_row = (
        "| no_actor_config | invisible to every dispatch round — the "
        "two dispatch-invisible legs, scoped |"
    )
    corrected_table = "\n".join((header, separator, marker_row, corrected_gloss_row))
    assert not _corpus_matches("virtual/table-clean.md", corrected_table), (
        "the corrected table's rows each pass on their own words"
    )
    # Round-5 F-2: the SAME laundering through the outer pipes' omission —
    # GFM/Python-Markdown accept a pipe-delimited row without its trailing
    # (and/or leading) delimiter, so demanding both outer pipes folded the
    # no-trailing-pipe rows into ONE paragraph block and the scoped
    # sibling's marker purified the gloss row again (the shipped-tree
    # attack: two such rows in observability.md's own metric table left
    # the tree-wide invariant green).
    open_scoped_row = (
        "| workflow_root_missing | the run cannot progress because its "
        "__flow__ root is gone (the dispatch-invisible rows are a separate "
        "class)"
    )
    open_gloss_row = "| " + frag_reasons + " | these rows " + "can never " + "be dispatched"
    attacked_open_table = "\n".join((header, separator, open_scoped_row, open_gloss_row))
    assert _corpus_matches("virtual/table-open-gloss.md", attacked_open_table), (
        "the no-trailing-pipe gloss row must red the scanner EVEN WHEN its "
        "no-trailing-pipe sibling carries the scoping marker — a pipe-"
        "delimited row is its own unit with or without the outer pipes"
    )
    assert _corpus_matches("virtual/row-open-alone.md", open_gloss_row), (
        "the no-trailing-pipe gloss row reds ALONE (its own unit, no sibling context)"
    )
    # The leading pipe may be omitted too (the delimiter row's own shape
    # admits it): the row is still its own unit.
    bare_gloss_row = frag_reasons + " | these rows can never be " + "dispatched"
    attacked_bare_table = "\n".join((header, separator, marker_row, bare_gloss_row))
    assert _corpus_matches("virtual/table-bare-gloss.md", attacked_bare_table), (
        "a no-leading-pipe gloss row in a table run is its own unit and reds"
    )
    # And the fully-corrected no-trailing-pipe table passes.
    corrected_open_row = (
        "| no_actor_config | invisible to every dispatch round — the "
        "two dispatch-invisible legs, scoped"
    )
    corrected_open_table = "\n".join((header, separator, open_scoped_row, corrected_open_row))
    assert not _corpus_matches("virtual/table-open-clean.md", corrected_open_table), (
        "the corrected no-trailing-pipe table's rows each pass on their own words"
    )


def test_html_rows_and_yaml_comments_are_their_own_units() -> None:
    """Round-6 M-1: the `.html` and YAML surfaces get the ruling's own
    semantic units — an HTML element/row and a comment statement are
    their own units, never blank-line-block folds — so DeepSeek's two
    executed defeats can no longer stay green by folding: two ADJACENT
    `<tr>` rows (the workers.html attack) and two ADJACENT YAML comment
    lines (the contrib rules.yaml attack) each red ALONE, and the
    blank-line-separated variants stay red. Honesty: a single multi-line
    element/comment that is genuinely one statement remains ONE unit (a
    wrapped sentence is never fragmented into meaningless pieces), and a
    scoping marker cannot leak across sibling elements. Every corpus
    value is assembled at runtime from independently non-triggering
    fragments (the module's own law)."""
    # ── the executed workers.html defeat: two ADJACENT <tr> rows ────────
    scoped_row = (
        "<tr><td>workflow_root_missing</td><td>the two dispatch-invisible legs are scoped</td></tr>"
    )
    gloss_row = (
        "<tr><td>"
        + frag_reasons
        + "</td><td>The stranded detector: pending/scheduled rows "
        + "can never "
        + "dispatch.</td></tr>"
    )
    adjacent = "<table>\n" + scoped_row + "\n" + gloss_row + "\n</table>"
    assert _corpus_matches("virtual/tr-adjacent.html", adjacent) == ["universal-no-dispatch"], (
        "the gloss row must red the scanner EVEN WHEN the adjacent row carries "
        "the scoping marker — an element row is its own unit"
    )
    separated = "<table>\n" + scoped_row + "\n\n" + gloss_row + "\n</table>"
    assert _corpus_matches("virtual/tr-separated.html", separated), (
        "the blank-line-separated variant stays red"
    )
    assert _corpus_matches("virtual/tr-alone.html", gloss_row), (
        "the gloss row reds ALONE (its own unit, no sibling context)"
    )
    # The units themselves: the two rows are separate element units — the
    # marker never leaks across them.
    row_units = [
        unit
        for unit in _corpus_units("virtual/tr-adjacent.html", adjacent)
        if unit.kind == "html-element" and unit.text.startswith("<tr>")
    ]
    assert len(row_units) == 2, [unit.text for unit in row_units]

    # ── the executed rules.yaml defeat: two ADJACENT comment lines ──────
    scoped_comment = "# the scoped read covers the two dispatch-invisible legs"
    gloss_comment = "# the stranded gauge: rows that " + "can never " + "dispatch"
    yaml_adjacent = "alert: probe\n" + scoped_comment + "\n" + gloss_comment + "\n"
    assert _corpus_matches("virtual/comments-adjacent.yaml", yaml_adjacent) == [
        "universal-no-dispatch"
    ], (
        "the gloss comment must red the scanner EVEN WHEN the adjacent comment "
        "line carries the scoping marker — a YAML comment line is its own unit"
    )
    yaml_separated = "alert: probe\n" + scoped_comment + "\n\n" + gloss_comment + "\n"
    assert _corpus_matches("virtual/comments-separated.yaml", yaml_separated), (
        "the blank-line-separated variant stays red"
    )
    assert _corpus_matches("virtual/comments-alone.yaml", gloss_comment), (
        "the gloss comment reds ALONE"
    )

    # ── honesty: a multi-line comment is genuinely ONE unit ────────────
    wrapped_comment = (
        "<!-- the stranded gauge's own note,\n   still one comment statement -->\n<td>plain</td>"
    )
    comment_units = [
        unit
        for unit in _corpus_units("virtual/comment-multiline.html", wrapped_comment)
        if unit.kind == "html-comment"
    ]
    assert len(comment_units) == 1, [unit.text for unit in comment_units]
    assert "still one comment statement" in comment_units[0].text
    jinja_comment = (
        "{# the stranded gauge's own note,\n   still one template comment #}\n<td>plain</td>"
    )
    template_units = [
        unit
        for unit in _corpus_units("virtual/comment-jinja.html", jinja_comment)
        if unit.kind == "template-comment"
    ]
    assert len(template_units) == 1, [unit.text for unit in template_units]
    assert "still one template comment" in template_units[0].text
    # ... and a multi-line comment carrying the gloss reds as its own unit.
    gloss_comment_doc = (
        "<!-- the stranded gauge: rows that " + "can never " + "dispatch,\n"
        "   wrapped across two lines -->\n"
        "<td>plain</td>"
    )
    assert _corpus_matches("virtual/comment-gloss.html", gloss_comment_doc), (
        "a multi-line comment carrying the gloss reds as its own unit"
    )

    # ── honesty: a wrapped sentence inside one element stays ONE unit ───
    wrapped_row = (
        "<table>\n"
        "<tr>\n"
        "  <td>the stranded gauge's own\n"
        "  wrapped sentence is one statement</td>\n"
        "</tr>\n"
        "</table>"
    )
    cell_units = [
        unit
        for unit in _corpus_units("virtual/tr-wrapped.html", wrapped_row)
        if unit.kind == "html-element" and unit.text.startswith("<td>")
    ]
    assert len(cell_units) == 1, [unit.text for unit in cell_units]
    assert "wrapped sentence is one statement" in cell_units[0].text


def test_markdown_structural_siblings_are_their_own_units() -> None:
    """Round-7 N-1: the Markdown surface gets the ruling's own structural
    units — each TIGHT LIST ITEM (nested markers included), each
    BLOCKQUOTE PARAGRAPH (a `>`-only separator line breaks it; `>>`
    nesting included), and each FENCED BLOCK (never merged with the
    surrounding prose; two adjacent fences separate; each fenced
    line-comment line its own unit) — so the four executed defeats can no
    longer stay green by folding: the architecture.md list-item
    insertion, the scoped+gloss bullet pair, the `> scoped` / `>` /
    `> gloss` pair, and the troubleshooting.md adjacent-comment-in-fence
    insertion each red AT THE GLOSS'S OWN UNIT. Honesty: a wrapped
    paragraph, a list item's own continuation lines, and a single fenced
    block's non-comment content stay the ONE unit they are (a statement
    is never fragmented into meaningless pieces; a fence's own
    comment-statement lines carry additional per-line units — the
    marker scan's over-take, which can only add reds). Every gloss value
    is assembled at runtime from independently non-triggering fragments
    (the module's own law)."""
    # ── the executed architecture.md defeat: a gloss list item inserted
    # into a tight list whose sibling item carries the scoping ───────────
    scoped_item = (
        "11. **Stranded-jobs detector**: runs every 60 s. Warns about "
        "pending/scheduled rows that cannot make progress, with three "
        "meanings: `" + frag_reasons + "` (the row is invisible to every "
        "dispatch round), `" + frag_unserved + "` (likewise never "
        "dispatched), and `workflow_root_missing` (the row may dispatch "
        "and run; the runbook owns the remedy)."
    )
    gloss_item = (
        "12. **Stranded rows**: The stranded gauge: rows that " + "can never " + "be dispatched."
    )
    attacked_list = scoped_item + "\n" + gloss_item
    assert _corpus_matches("virtual/list-gloss.md", attacked_list) == ["universal-no-dispatch"], (
        "the gloss item must red the scanner EVEN WHEN the sibling item "
        "carries the scoping markers — a tight list item is its own unit"
    )
    assert _corpus_matches("virtual/list-gloss-separated.md", scoped_item + "\n\n" + gloss_item), (
        "the blank-line-separated variant stays red"
    )
    assert _corpus_matches("virtual/list-gloss-alone.md", gloss_item), (
        "the gloss item reds ALONE (its own unit, no sibling context)"
    )
    # The units themselves: the two items are separate units — the
    # sibling's markers never leak across them.
    item_units = [
        unit
        for unit in _corpus_units("virtual/list-gloss.md", attacked_list)
        if unit.text.lstrip().startswith(("11.", "12."))
    ]
    assert len(item_units) == 2, [unit.text for unit in item_units]
    # The fully-corrected item passes on its own words.
    corrected_item = (
        "12. **Stranded rows**: the rows the gauge reports are scoped to "
        "the two dispatch-invisible legs, plus workflow_root_missing's "
        "run-scoped class."
    )
    assert not _corpus_matches("virtual/list-clean.md", scoped_item + "\n" + corrected_item), (
        "the corrected list's items each pass on their own words"
    )
    # A NESTED marker is its own unit too: the parent's scoping cannot
    # purify a nested gloss.
    nested_gloss = "    - The stranded gauge: rows that " + "can never " + "be dispatched."
    attacked_nested = scoped_item + "\n" + nested_gloss
    assert _corpus_matches("virtual/list-nested-gloss.md", attacked_nested) == [
        "universal-no-dispatch"
    ], "a nested list item is its own unit — the parent's markers never purify it"
    nested_units = [
        unit
        for unit in _corpus_units("virtual/list-nested-gloss.md", attacked_nested)
        if unit.text.lstrip().startswith(("11.", "-"))
    ]
    assert len(nested_units) == 2, [unit.text for unit in nested_units]

    # ── the executed scoped+gloss bullet pair (a tight bullet list) ──────
    scoped_bullet = (
        "- The dispatch-invisible legs: " + frag_reasons + " and " + frag_unserved + " are scoped."
    )
    gloss_bullet = "- The stranded gauge: rows that " + "can never " + "be dispatched."
    assert _corpus_matches("virtual/bullet-pair.md", scoped_bullet + "\n" + gloss_bullet) == [
        "universal-no-dispatch"
    ], "the gloss bullet must red EVEN WHEN the adjacent bullet carries the marker"
    assert _corpus_matches("virtual/bullet-separated.md", scoped_bullet + "\n\n" + gloss_bullet), (
        "the blank-line-separated variant stays red"
    )

    # ── the executed blockquote defeat: `> scoped` / `>` / `> gloss` ─────
    scoped_quote = (
        "> The dispatch-invisible legs: " + frag_reasons + " and " + frag_unserved + " are scoped."
    )
    gloss_quote = "> The stranded gauge: rows that " + "can never " + "be dispatched."
    quote_pair = scoped_quote + "\n>\n" + gloss_quote
    assert _corpus_matches("virtual/quote-pair.md", quote_pair) == ["universal-no-dispatch"], (
        "the gloss quote paragraph must red the scanner EVEN WHEN the "
        "sibling paragraph carries the scoping marker — a `>`-only "
        "separator line breaks the paragraph into the next unit"
    )
    # The two paragraphs are separate units — and the `>>` nested form too.
    quote_units = [
        unit
        for unit in _corpus_units("virtual/quote-pair.md", quote_pair)
        if "dispatch-invisible" in unit.text or "can never" in unit.text
    ]
    assert len(quote_units) == 2, [unit.text for unit in quote_units]
    nested_quote_pair = quote_pair.replace("> ", ">> ")
    assert _corpus_matches("virtual/quote-nested-pair.md", nested_quote_pair) == [
        "universal-no-dispatch"
    ], "the `>>`-nested blockquote's paragraphs split the same way"

    # ── the executed troubleshooting.md defeat: two ADJACENT `--` comment
    # lines at the top of the shipped sql fence ──────────────────────────
    gloss_comment = "-- the stranded gauge: rows that " + "can never " + "dispatch"
    scoped_comment = "-- scoped to the two dispatch-invisible legs only"
    fence_adjacent = "```sql\n" + gloss_comment + "\n" + scoped_comment + "\nSELECT 1;\n```"
    assert _corpus_matches("virtual/fence-comments-adjacent.md", fence_adjacent) == [
        "universal-no-dispatch"
    ], (
        "the gloss fence comment must red the scanner EVEN WHEN the adjacent "
        "comment line carries the scoping marker — a fence's line-comment "
        "line is its own unit"
    )
    comment_units = [
        unit
        for unit in _corpus_units("virtual/fence-comments-adjacent.md", fence_adjacent)
        if unit.text.strip().startswith("--")
    ]
    assert len(comment_units) == 2, [unit.text for unit in comment_units]
    assert _corpus_matches(
        "virtual/fence-comments-separated.md",
        "```sql\n" + gloss_comment + "\n\n" + scoped_comment + "\n```",
    ), "the blank-line-separated variant stays red"
    assert _corpus_matches("virtual/fence-comment-alone.md", gloss_comment), (
        "the gloss fence comment reds ALONE"
    )
    # Honesty: a single fenced block's non-comment content is the ONE
    # statement it is (delimiters included), two adjacent fences are
    # separate units, and a fence never merges with the surrounding prose
    # (the prose's own scoping marker cannot purify the fence's gloss).
    plain_fence = "```sql\nSELECT a\nFROM b\nWHERE c > d;\n```"
    plain_units = _corpus_units("virtual/fence-plain.md", plain_fence)
    fence_unit = [u for u in plain_units if "```" in u.text]
    assert len(fence_unit) == 1, [unit.text for unit in plain_units]
    # The trailing `;` is a scanned comment marker wherever it sits, so
    # the statement's last line ALSO carries a per-line comment-statement
    # unit (the no-whitespace marker scan's over-take: an extra unit can
    # only add a red, never hide one — the fence's own unit still carries
    # the whole content).
    overtake = [u for u in plain_units if u.text.strip() == "WHERE c > d;"]
    assert len(overtake) == 1 and (overtake[0].start_line, overtake[0].end_line) == (4, 4), [
        unit.text for unit in plain_units
    ]
    # The exact total: the fence unit plus the one per-line over-take
    # unit — a third unexpected unit has no place to hide (the same pin
    # the two-fences arm below carries).
    assert len(plain_units) == 2, [unit.text for unit in plain_units]
    two_fences = "```sql\nSELECT 1;\n```\n```sql\nSELECT 2;\n```"
    fence_units = _corpus_units("virtual/fence-two.md", two_fences)
    two_fence_units = [u for u in fence_units if u.text.count("```") >= 1]
    assert len(two_fence_units) == 2, [unit.text for unit in fence_units]
    # The exact total: two fence units plus the two `SELECT n;` per-line
    # over-take units the no-whitespace marker scan adds — an unexpected
    # unit has no place to hide.
    assert len(fence_units) == 4, [unit.text for unit in fence_units]
    prose_then_fence = (
        "The dispatch-invisible legs are scoped.\n" + "```sql\n" + gloss_comment + "\n```"
    )
    prose_fence_units = _corpus_units("virtual/fence-prose-gloss.md", prose_then_fence)
    firing_units = [unit for unit in prose_fence_units if any(rule(unit) for _n, rule in RULES)]
    assert any(unit.text.strip() == gloss_comment for unit in firing_units), (
        "a fence never merges with its surrounding prose — the prose's marker "
        f"cannot purify the fence's gloss comment's own unit: {[u.text for u in firing_units]}"
    )


def test_indented_fences_inside_containers_are_their_own_units() -> None:
    """Round-8 D8-1: a fence indented to a list item's — or an MkDocs
    admonition's — content column is a FENCE, not paragraph continuation
    text. The hand-rolled splitter no longer recognised it as a fence at
    that indentation, so its body folded into the enclosing paragraph
    and a scoped SQL comment inside it purified the prose universal
    gloss (`_scan` stayed green on one unit). The parser's token/line
    map separates them at any container depth: the gloss paragraph and
    the fence are leaf blocks of their own at their own true spans, so
    the gloss reds AT ITS OWN UNIT and the fence's scoped comment can
    never purify it. Every gloss value is assembled at runtime from
    independently non-triggering fragments (the module's own law)."""
    gloss = "The stranded gauge: rows that " + "can never " + "be dispatched."
    scoped_comment = "-- scoped to the two dispatch-invisible legs only"
    gloss_comment = "-- the stranded gauge: rows that " + "can never " + "be dispatched"

    # ── the executed D8-1 shape: an ordered list item whose fence is
    # indented to the item's content column ──────────────────────────────
    item_fence = (
        "1. **Stranded rows**: " + gloss + "\n"
        "    ```sql\n"
        "    " + scoped_comment + "\n"
        "    SELECT 1;\n"
        "    ```"
    )
    assert _corpus_matches("virtual/item-fence.md", item_fence) == ["universal-no-dispatch"], (
        "the gloss item must red EVEN WHEN the fence inside it carries the "
        "scoping comment — a 4-space fence in a list item is its own block"
    )
    units = _corpus_units("virtual/item-fence.md", item_fence)
    gloss_units = [unit for unit in units if "can never" in unit.text]
    assert len(gloss_units) == 1, [unit.text for unit in units]
    assert (gloss_units[0].start_line, gloss_units[0].end_line) == (1, 1), (
        "the gloss's own unit is the item's own paragraph line"
    )
    fence_units = [unit for unit in units if unit.text.count("```") >= 1 and "SELECT" in unit.text]
    assert len(fence_units) == 1 and (fence_units[0].start_line, fence_units[0].end_line) == (
        2,
        5,
    ), [unit.text for unit in units]
    # ... and the corrected item (the gloss scoped in its own words) passes.
    corrected_item = (
        "1. **Stranded rows**: the rows the gauge reports are scoped to "
        "the two dispatch-invisible legs, plus workflow_root_missing's "
        "run-scoped class.\n"
        "    ```sql\n"
        "    " + scoped_comment + "\n"
        "    SELECT 1;\n"
        "    ```"
    )
    assert not _corpus_matches("virtual/item-fence-clean.md", corrected_item)

    # ── the shipped observability.md shape: the fence inside an MkDocs
    # admonition, with the scoped+gloss comment pair inserted at its top ──
    admonition_pair = (
        '!!! tip "Which job is starving?"\n'
        "\n"
        "    ```sql\n"
        "    " + gloss_comment + "\n"
        "    " + scoped_comment + "\n"
        "    SELECT 1;\n"
        "    ```"
    )
    assert _corpus_matches("virtual/admonition-fence-gloss.md", admonition_pair) == [
        "universal-no-dispatch"
    ], (
        "the gloss comment inside the admonition's 4-space fence must red "
        "EVEN WHEN its sibling comment carries the scoping marker"
    )
    admonition_units = _corpus_units("virtual/admonition-fence-gloss.md", admonition_pair)
    comment_units = [u for u in admonition_units if u.text.strip().startswith("--")]
    assert len(comment_units) == 2, [unit.text for unit in admonition_units]
    gloss_comment_units = [u for u in comment_units if "can never" in u.text]
    assert len(gloss_comment_units) == 1
    assert (gloss_comment_units[0].start_line, gloss_comment_units[0].end_line) == (4, 4), (
        "the gloss's own unit is its own comment line inside the fence"
    )
    # The code block itself is its own unit at its own span — never merged
    # with the `!!! tip` paragraph line above it. (The bare `SELECT 1;`
    # line now carries a per-line comment-statement unit of its own — a
    # `;` is a scanned comment marker wherever it sits, so the filter
    # selects the FENCE unit by its own fence text.)
    code_units = [u for u in admonition_units if u.text.count("```") >= 1 and "SELECT 1" in u.text]
    assert len(code_units) == 1 and (code_units[0].start_line, code_units[0].end_line) == (3, 7), [
        unit.text for unit in admonition_units
    ]
    select_units = [u for u in admonition_units if u.text.strip() == "SELECT 1;"]
    assert len(select_units) == 1 and (select_units[0].start_line, select_units[0].end_line) == (
        6,
        6,
    ), (
        "the bare `SELECT 1;` line's own comment-statement unit is the "
        "over-take the no-whitespace marker scan adds — an extra unit can "
        "only add a red, never hide one"
    )


def test_lazy_continuation_belongs_to_its_paragraph() -> None:
    """Round-8 D8-2: a legal lazy-continuation line — a line indented
    below the list item's content column that starts no new block — is
    part of the item's paragraph, because the parser says so. The
    hand-rolled splitter detached it into a paragraph of its own, so the
    CORRECTED text (the scoping on the continuation line) false-red.
    The parser's paragraph map keeps the two lines one statement: the
    corrected text is green, and the uncorrected item still reds."""
    corrected = (
        "12. **Stranded rows**: The stranded gauge: rows that " + "can never " + "be dispatched,\n"
        "   scoped to the two dispatch-invisible legs only."
    )
    assert not _corpus_matches("virtual/lazy-corrected.md", corrected), (
        "the lazy continuation is the paragraph's own second line — the "
        "corrected text must be green, never a false red"
    )
    units = _corpus_units("virtual/lazy-corrected.md", corrected)
    assert len(units) == 1, [unit.text for unit in units]
    assert (units[0].start_line, units[0].end_line) == (1, 2)
    gloss_item = (
        "12. **Stranded rows**: The stranded gauge: rows that " + "can never " + "be dispatched."
    )
    assert _corpus_matches("virtual/lazy-gloss.md", gloss_item) == ["universal-no-dispatch"], (
        "the uncorrected item still reds"
    )


def test_setext_and_thematic_breaks_separate_their_neighbours() -> None:
    """Round-8 D8-3: a setext underline and a thematic break are BLOCKS of
    their own — they end the scoped unit and start the next one. The
    hand-rolled splitter folded `scoped.` / `---` / gloss into ONE
    unit (no blank line, no marker between them) and the marker purified
    the gloss. The parser's heading/hr token maps separate them."""
    scoped = "The dispatch-invisible legs are scoped."
    gloss = "The stranded gauge: rows that " + "can never " + "be dispatched."
    setext_pair = scoped + "\n---\n" + gloss
    assert _corpus_matches("virtual/setext-gloss.md", setext_pair) == ["universal-no-dispatch"], (
        "the setext heading ends its own unit — the gloss after it reds ALONE"
    )
    units = _corpus_units("virtual/setext-gloss.md", setext_pair)
    gloss_units = [unit for unit in units if "can never" in unit.text]
    assert len(gloss_units) == 1 and (gloss_units[0].start_line, gloss_units[0].end_line) == (
        3,
        3,
    ), [unit.text for unit in units]
    hr_pair = scoped + "\n\n---\n\n" + gloss
    assert _corpus_matches("virtual/hr-gloss.md", hr_pair) == ["universal-no-dispatch"], (
        "a thematic break separates its neighbours the same way"
    )
    hr_units = _corpus_units("virtual/hr-gloss.md", hr_pair)
    hr_lines = sorted(
        {unit.start_line for unit in hr_units if unit.start_line == unit.end_line == 3}
    )
    assert hr_lines == [3], [unit.text for unit in hr_units]


def test_md_html_comments_are_their_own_units() -> None:
    """Round-8 D8-4: a `.md` file's `<!-- -->` comments get the same
    treatment the `.html` comment units give — each comment statement
    its own unit at its own span — because the parser's html_block
    tokens own them: two ADJACENT comments are two sibling blocks, never
    one folded unit (the old `_units_for` dispatch ran no comment pass
    on `.md`, and the blank-line block folded the pair). A multi-line
    comment stays the ONE statement it is, and a comment INLINE in a
    paragraph is part of that paragraph's own statement (the same
    honesty law as a wrapped sentence)."""
    scoped = "<!-- the scoped read covers the two dispatch-invisible legs -->"
    gloss = "<!-- the stranded gauge: rows that " + "can never " + "be dispatched -->"
    adjacent = scoped + "\n" + gloss
    assert _corpus_matches("virtual/md-comments-adjacent.md", adjacent) == [
        "universal-no-dispatch"
    ], "the gloss comment must red EVEN WHEN the adjacent comment carries the scoping marker"
    units = _corpus_units("virtual/md-comments-adjacent.md", adjacent)
    comment_blocks = [unit for unit in units if unit.text.startswith("<!--")]
    assert len(comment_blocks) == 2, [unit.text for unit in units]
    gloss_blocks = [unit for unit in comment_blocks if "can never" in unit.text]
    assert len(gloss_blocks) == 1
    assert (gloss_blocks[0].start_line, gloss_blocks[0].end_line) == (2, 2), (
        "the gloss's own unit is its own comment block"
    )
    assert _corpus_matches("virtual/md-comments-separated.md", scoped + "\n\n" + gloss), (
        "the blank-line-separated variant stays red"
    )
    wrapped = "<!-- the stranded gauge's own note,\n   still one comment statement -->\nprose line"
    wrapped_units = [
        unit
        for unit in _corpus_units("virtual/md-comment-multiline.md", wrapped)
        if unit.text.startswith("<!--")
    ]
    assert len(wrapped_units) == 1 and "still one comment statement" in wrapped_units[0].text, [
        unit.text for unit in wrapped_units
    ]


def test_span_digest_is_total_on_lone_surrogates() -> None:
    """Round-8 D8-5: a lone surrogate is a legal Python string constant
    (a source escape such as ``\\ud800`` survives `ast.parse` into the
    constant's value — the shipped `tests/test_obs_exception_redaction.py`
    carries two), and the digest was a bare UTF-8 encode of the unit's
    text, so digesting such a unit raised UnicodeEncodeError. The digest
    is now total ("surrogatepass"): the real file's surrogate units
    digest, and a surrogate span can be allowlisted and verified live
    through the anti-rot law end to end (pre-fix, deriving the entry's
    digest crashed; the defeat's red is preserved in the round-8 logs)."""
    surrogate_file = "tests/test_obs_exception_redaction.py"
    raw = (REPO_ROOT / surrogate_file).read_text(encoding="utf-8")
    tree = ast.parse(raw)
    surrogate_units = [
        unit
        for unit in _python_units(surrogate_file, raw, tree)
        if any(0xD800 <= ord(char) <= 0xDFFF for char in unit.text)
    ]
    assert len(surrogate_units) == 2, (
        "the shipped surrogate-bearing string constants must still be units"
    )
    digests = [unit.span_digest for unit in surrogate_units]  # must not raise
    assert all(len(digest) == 64 and int(digest, 16) >= 0 for digest in digests)
    # End to end through the anti-rot law: an entry derived from the
    # surrogate unit's digest verifies live against the same file. The
    # strengthened law (round-12 D12-5) also demands the entry's live
    # unit FIRE a rule; the surrogate string is an exception-redaction
    # fixture and fires none, so the law's honest verdict for this
    # synthetic entry is exactly ONE stale-suppression violation — and
    # that verdict still proves the D8-5 property end to end: it is
    # reachable only because the digest matched its live unit (a digest
    # miss would have reported "matches NO live unit" instead, and a
    # non-total digest would have raised above).
    entries = ((digests[0], surrogate_file, "the surrogate digest is total"),)
    violations = _allowlist_rotations(entries, file_text=lambda _rel: raw)
    assert (
        len(violations) == 1
        and "fires NO rule" in violations[0]
        and surrogate_file in violations[0]
    ), violations


def test_markdown_units_never_partially_overlap() -> None:
    """The unit model's stability law, driven over every shipped file
    that carries markdown units and over the corpus shapes: the markdown
    units' text integrity and pairwise overlap are delegated to the
    module's own STRUCTURAL LAWS (`_truncation_violations` — a unit's
    text is exactly the file's own raw lines minus its container dedent
    — and `_overlap_violations` — the GLOBAL disjoint-or-contained law,
    every pair, not merely adjacent pairs, which was the round-9 D9-8
    defect). The shipped docs' block tokens are also pinned to the
    modelled closed set: the parser growing a block construct the model
    does not know fails closed in `_markdown_block_units`, and this pin
    fails closed FIRST if the shipped tree already uses one."""
    modelled = (
        _CONTAINER_TOKENS
        | _LEAF_TOKENS
        # The table's inner openers ride the table_open span (the rows
        # are emitted per line across that span; the inner tokens are
        # never units of their own).
        | {"table_open", "thead_open", "tbody_open", "tr_open", "th_open", "td_open"}
        | {"inline", "table_close"}
    )

    def check(rel: str, raw: str) -> None:
        units = [unit for unit in _units_for(rel, raw) if unit.kind == "markdown"]
        violations = _truncation_violations(rel, raw, units) + _overlap_violations(rel, units)
        assert not violations, f"{rel}:\n" + "\n".join(f"  {v}" for v in violations)
        for token in _MD_PARSER.parse(raw):
            if token.nesting == -1:
                continue
            if token.type == "inline":
                continue
            assert token.type in modelled, (
                f"{rel}: block token {token.type!r} outside the modelled closed set"
            )

    for path in _iter_scan_files(REPO_ROOT):
        if path.suffix not in (".md", ".yml", ".yaml", ".html"):
            continue
        rel = str(path.relative_to(REPO_ROOT))
        check(rel, path.read_text(encoding="utf-8"))
    check("virtual/item-fence.md", "1. item\n    ```sql\n    SELECT 1;\n    ```")
    check("virtual/setext-gloss.md", "scoped.\n---\ngloss")
    check("virtual/md-comments-adjacent.md", "<!-- scoped -->\n<!-- gloss -->")


def test_unit_model_laws_hold_over_every_scanned_file() -> None:
    """The unit model's CHECKED LAWS over every scanned file (the
    round-9 ruling: replace per-construct enumeration with a checked
    property, so a construct no round thought of cannot leave its text
    outside the units — what a unit's own interior waives is the
    scoping predicate's declared business):

    * LAW 1 line coverage — every non-blank line of a text-carrier file
      (`.md`/`.yml`/`.yaml`/`.html`) belongs to at least one unit; for
      `.py`, every tokenize-level STRING/COMMENT token (a Python file's
      only text carriers) is owned by a unit;
    * LAW 2 disjoint-or-contained — every pair of a file's units is
      disjoint or one contains the other, checked GLOBALLY;
    * LAW 3 no truncation — a unit's text is consistent with its
      declared line span (exactly the raw lines minus the container
      dedent; an exact substring for statement units; the parsed value
      occurring in its span for YAML).

    The per-line floors (markdown-text / yaml-text / html-text) are what
    make LAW 1 true; this test is what makes losing a floor — or any
    other model regression that hides text — red."""
    files = 0
    for path in _iter_scan_files(REPO_ROOT):
        rel = str(path.relative_to(REPO_ROOT))
        raw = path.read_text(encoding="utf-8")
        units = _units_for(rel, raw)
        violations = (
            _coverage_violations(rel, raw, units)
            + _overlap_violations(rel, units)
            + _truncation_violations(rel, raw, units)
        )
        assert not violations, f"{rel}:\n" + "\n".join(f"  {v}" for v in violations)
        files += 1
    assert files > 1000, f"the laws ran over {files} files — the scan scope collapsed?"


def test_unit_model_laws_red_when_violated() -> None:
    """Each law REDS on an injected violation (runtime-constructed unit
    lists — the tree is never touched), and the laminar check agrees
    with the brute-force all-pairs form on randomized inputs, including
    the nested-crossing shapes the round-10 F10-1 defect showed the old
    running-maximum sweep missing."""
    text = "first line has the scoping marker\nsecond line hides a gloss\n"
    # LAW 1: a line outside every unit reds, naming the file and line.
    units = [Unit("virtual/laws.md", "markdown", 1, 1, "first line has the scoping marker")]
    violations = _coverage_violations("virtual/laws.md", text, units)
    assert any("virtual/laws.md:2" in v for v in violations), violations

    # ... and the same units against a fully-covered text are clean.
    assert _coverage_violations("virtual/laws.md", "only one line\n", units) == [], (
        _coverage_violations("virtual/laws.md", "only one line\n", units)
    )

    # LAW 2: a partial overlap reds (unit A crosses unit B's boundary).
    overlapping = [
        Unit("virtual/laws.md", "markdown", 1, 3, "a\nb\nc"),
        Unit("virtual/laws.md", "markdown", 2, 4, "b\nc\nd"),
    ]
    violations = _overlap_violations("virtual/laws.md", overlapping)
    assert any("1-3" in v and "2-4" in v for v in violations), violations
    # ... contained and disjoint shapes are clean.
    assert (
        _overlap_violations(
            "virtual/laws.md",
            [
                Unit("virtual/laws.md", "markdown", 1, 4, "a\nb\nc\nd"),
                Unit("virtual/laws.md", "markdown", 2, 3, "b\nc"),
                Unit("virtual/laws.md", "markdown", 6, 7, "e\nf"),
            ],
        )
        == []
    ), _overlap_violations("virtual/laws.md", [])
    # ... the F10-1 defeats RED: two units both contained in a third,
    # crossing each other UNDER the container's end — the running-
    # maximum sweep compared each unit only with the interval owning
    # the maximum end and reported all three shapes clean.
    under_max = [
        Unit("virtual/laws.md", "markdown", 1, 10, "a" * 10),
        Unit("virtual/laws.md", "markdown", 2, 6, "b" * 5),
        Unit("virtual/laws.md", "markdown", 4, 8, "c" * 5),
    ]
    violations = _overlap_violations("virtual/laws.md", under_max)
    assert any("2-6" in v and "4-8" in v for v in violations), violations
    # ... and the round-8 adjacent-check shape reds too (the old sweep
    # missed it as well).
    round8 = [
        Unit("virtual/laws.md", "markdown", 1, 10, "a" * 10),
        Unit("virtual/laws.md", "markdown", 2, 6, "b" * 5),
        Unit("virtual/laws.md", "markdown", 5, 7, "c" * 3),
    ]
    violations = _overlap_violations("virtual/laws.md", round8)
    assert any("2-6" in v and "5-7" in v for v in violations), violations
    # ... and the check agrees with the brute-force all-pairs law on
    # randomized inputs — a seeded fuzz whose generator produces
    # nested-crossing shapes (crossing pairs inside a containing unit)
    # on purpose, plus fully random spans, so the agreement is evidence
    # and not one hand-picked set.
    import itertools
    import random

    def brute_force(units: list[Unit]) -> bool:
        for a, b in itertools.combinations(units, 2):
            overlap = not (a.end_line < b.start_line or b.end_line < a.start_line)
            contains = (a.start_line <= b.start_line and b.end_line <= a.end_line) or (
                b.start_line <= a.start_line and a.end_line <= b.end_line
            )
            touch = a.start_line == b.end_line or b.start_line == a.end_line
            if overlap and not contains and not touch:
                return True
        return False

    def fuzz_family(rng: random.Random) -> list[Unit]:
        units: list[Unit] = []
        # A nest of containers, then crossing pairs INSIDE the
        # innermost container (the under-max shape), at random depths.
        open_start, open_end = 1, rng.randint(6, 20)
        units.append(Unit("virtual/laws.md", "markdown", open_start, open_end, "outer"))
        for _ in range(rng.randint(0, 3)):
            inner_start = open_start + rng.randint(0, 2)
            inner_end = open_end - rng.randint(0, 2)
            if inner_start > inner_end:
                break
            units.append(Unit("virtual/laws.md", "markdown", inner_start, inner_end, "inner"))
            open_start, open_end = inner_start, inner_end
        for _ in range(rng.randint(1, 3)):
            a_start = open_start + rng.randint(0, max(0, open_end - open_start - 2))
            a_end = a_start + rng.randint(0, open_end - a_start)
            b_start = open_start + rng.randint(0, max(0, open_end - open_start - 2))
            b_end = b_start + rng.randint(0, open_end - b_start)
            units.append(Unit("virtual/laws.md", "markdown", a_start, a_end, "a"))
            units.append(Unit("virtual/laws.md", "markdown", b_start, b_end, "b"))
        return units

    # Why: reproducible fuzz schedules, not cryptography.
    rng = random.Random(20261117)  # noqa: S311
    for _ in range(500):
        family = fuzz_family(rng)
        assert bool(_overlap_violations("virtual/laws.md", family)) == brute_force(family), [
            (unit.start_line, unit.end_line) for unit in family
        ]
        loose = [
            Unit(
                "virtual/laws.md",
                "markdown",
                (start := rng.randint(1, 12)),
                start + rng.randint(0, 8),
                "t",
            )
            for _ in range(rng.randint(2, 7))
        ]
        assert bool(_overlap_violations("virtual/laws.md", loose)) == brute_force(loose), [
            (unit.start_line, unit.end_line) for unit in loose
        ]

    # LAW 3: a truncated unit reds (the D9-5 class: text lost relative
    # to its span).
    truncated = [Unit("virtual/laws.md", "markdown", 1, 2, "first line has the scoping marker")]
    violations = _truncation_violations("virtual/laws.md", text, truncated)
    assert any("1-2" in v for v in violations), violations
    # ... a container-dedent unit is exactly the raw lines minus its
    # dedent — every non-blank line loses the dedent's leading columns
    # (the renderer's detab applies per line; the law reconstructs, not
    # merely compares).
    assert (
        _truncation_violations(
            "virtual/laws.md",
            text,
            [
                Unit(
                    "virtual/laws.md",
                    "markdown",
                    1,
                    2,
                    "irst line has the scoping marker\necond line hides a gloss",
                    1,
                )
            ],
        )
        == []
    )
    # ... and a yaml unit whose parsed value is foreign to its span reds.
    yaml_text = "key: the shipped value\n"
    yaml_units = [Unit("virtual/laws.yaml", "yaml", 1, 1, "a foreign folded value")]
    assert any(
        "does not occur" in v
        for v in _truncation_violations("virtual/laws.yaml", yaml_text, yaml_units)
    )
    assert (
        _truncation_violations(
            "virtual/laws.yaml",
            yaml_text,
            [Unit("virtual/laws.yaml", "yaml", 1, 1, "the shipped value")],
        )
        == []
    )


def test_hidden_text_inside_unsplit_characters_is_covered() -> None:
    """Round-9 D9-5's own class: `str.splitlines` splits on \\v, \\f,
    \\x85, U+2028 and U+2029 — the parser does not. The unit model now
    splits on "\\n" ONLY (`_split_lines`), so a paragraph line carrying
    one of those characters keeps its WHOLE text inside the paragraph's
    own unit — no text hides outside every unit — and the file's line
    grid agrees with the parser's (the coverage law holds where the old
    model left a non-blank line outside every unit)."""
    scoped_line = "The dispatch-invisible legs are scoped."
    gloss = "The stranded gauge: rows that " + "can never " + "be dispatched."
    for separator in ("\x0c", "\x0b", "\x85", "", ""):
        text = f"{scoped_line}{separator}{gloss}.\n"
        units = _corpus_units("virtual/unsplit.md", text)
        # The parser sees ONE line and ONE paragraph; the model's line
        # grid agrees, and the paragraph's unit carries the WHOLE line —
        # the gloss is inside a unit, never outside one.
        assert len(units) == 1, [unit.text for unit in units]
        assert (units[0].start_line, units[0].end_line) == (1, 1)
        assert gloss in units[0].text
        assert separator in units[0].text
        # The coverage law holds (under the old splitlines grid the
        # gloss's half-line was outside every unit — the pre-fix red is
        # preserved in the round-9 evidence logs).
        assert _coverage_violations("virtual/unsplit.md", text, list(units)) == []


def test_container_body_dedent_is_the_renderer_column_slice() -> None:
    """Round-10 F10-5: the container body's dedent was a CHARACTER slice
    of the raw line while the renderer's detab law counts a tab as four
    COLUMNS (its NormalizeWhitespace preprocessor expands every tab to
    the next 4-column tab stop before any block processor runs) — a
    tab-indented body line's text was mangled: under an 8-column
    content boundary, a `stranded rows` line's universal tail sliced
    mid-variant (the detector mention AND the `can never …` tail both
    broken by the slice), the gloss hidden, and the no-truncation law
    SHARED the slice so it could not see the mangle. The model and the
    law now run on the renderer's own tab-expanded grid
    (`_renderer_lines`): the tab-indented body line keeps its text, the
    gloss fires at its own unit, and the law's reconstruction
    agrees."""
    doc = '!!! note "T"\n\t!!! note "T2"\n\t\t\t' + "stranded rows " + "can never " + "dispatch\n"
    assert _corpus_matches("virtual/tab-nested.md", doc) == ["universal-no-dispatch"], (
        "the tab-indented body line's gloss must fire — the renderer "
        "expands the tabs, so the detab slice keeps the whole line's text"
    )
    units = [
        unit for unit in _corpus_units("virtual/tab-nested.md", doc) if "stranded" in unit.text
    ]
    assert len(units) == 1 and (units[0].start_line, units[0].end_line) == (3, 3), [
        (unit.start_line, unit.end_line, unit.text[:50]) for unit in units
    ]
    assert "can never dispatch" in units[0].text
    # The no-truncation law reconstructs the SAME text from the raw line
    # minus the declared dedent (8 columns = the inner container's
    # content boundary) — the law sees the real slice now.
    assert (
        _truncation_violations(
            "virtual/tab-nested.md",
            doc,
            [u for u in _corpus_units("virtual/tab-nested.md", doc) if u.kind == "markdown"],
        )
        == []
    )


def test_quoted_containers_get_their_own_body_paragraphs() -> None:
    """Round-10 F10-2: a blockquote-prefixed container (`> !!! note`)
    was not a container at all — the scanner left it at the
    blockquote's own paragraph granularity, and ONE unit folded the
    container's body paragraphs, which the renderer renders as separate
    `<p>`s of the admonition div (it strips the blockquote prefix per
    line, re-runs the block processors on the cleaned chunk, and the
    admonition's detab law splits the body). A scoped first paragraph
    purified a universal gloss in the second. The model now matches the
    marker after the renderer's blockquote clean law and runs the same
    body law on the cleaned lines — for `!!!`, `???` and `===` alike —
    so the quoted form gets the same body paragraph units as the
    unquoted form, and the `>`-only separator line stays a floor unit.
    The end-to-end face drives the REAL `_scan` over a runtime corpus:
    the gloss in the second body paragraph is an unallowlisted failure
    naming the file. Every gloss value is assembled at runtime from
    independently non-triggering fragments (the module's own law)."""
    scoped = "The two dispatch-invisible legs are scoped"
    gloss = "The stranded gauge: rows that " + "can never " + "be dispatched"
    for name, marker in (
        ("quoted-admonition", '!!! note "T"'),
        ("quoted-details", '??? note "T"'),
        ("quoted-tab", '=== "T"'),
    ):
        doc = f"> {marker}\n>\n>     {scoped}.\n>\n>     {gloss}.\n"
        assert _corpus_matches(f"virtual/{name}.md", doc) == ["universal-no-dispatch"], (
            f"the {marker} gloss in the quoted container's SECOND body "
            "paragraph must red — the renderer renders it as the "
            "admonition's own second <p>"
        )
        units = [
            unit
            for unit in _corpus_units(f"virtual/{name}.md", doc)
            if unit.kind == "markdown" and "can never" in unit.text
        ]
        assert len(units) == 1 and units[0].start_line == 5, [
            (unit.start_line, unit.end_line, unit.text[:40]) for unit in units
        ]
        # The `>`-only separator line is its own floor unit — coverage
        # never depends on the container swallowing it.
        floor = [
            unit
            for unit in _corpus_units(f"virtual/{name}.md", doc)
            if unit.kind == "markdown-text"
        ]
        assert [unit.start_line for unit in floor] == [2, 4], [
            (unit.start_line, unit.text) for unit in floor
        ]
    # The corrected quoted form passes (each body paragraph its own words).
    corrected = (
        '> !!! note "T"\n'
        ">\n"
        ">     The read is scoped to the two dispatch-invisible legs.\n"
        ">\n"
        ">     The gauge's rows are scoped to the two dispatch-invisible\n"
        ">     legs plus the run-scoped class.\n"
    )
    assert not _corpus_matches("virtual/quoted-clean.md", corrected)
    # End to end through the REAL scanner entry point over a runtime
    # corpus (nothing under the real docs/, src/ or tests/ is created):
    # the quoted container's second-paragraph gloss is a failure naming
    # the file, while the same gloss unquoted is caught the same way —
    # the quoted form no longer hides.
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for scan_root in SCAN_ROOTS:
            (root / scan_root).mkdir()
        quoted = root / "docs" / "quoted.md"
        quoted.write_text(
            '> !!! note "T"\n'
            ">\n"
            ">     The two dispatch-invisible legs are scoped.\n"
            ">\n"
            ">     " + gloss + ".\n",
            encoding="utf-8",
        )
        failures = _scan(root)
        assert any(
            path == "docs/quoted.md" and rule == "universal-no-dispatch"
            for rule, _unit, path in failures
        ), failures
        unquoted = root / "docs" / "quoted.md"
        unquoted.write_text(
            '!!! note "T"\n\nThe two dispatch-invisible legs are scoped.\n\n' + gloss + ".\n",
            encoding="utf-8",
        )
        failures = _scan(root)
        assert any(
            path == "docs/quoted.md" and rule == "universal-no-dispatch"
            for rule, _unit, path in failures
        ), failures


def test_container_bodies_are_markdown_at_their_own_lines() -> None:
    """Round-9 D9-1: markdown-it has no MkDocs admonition/tab extension,
    so a `!!!`/`???`/`===` container's body parsed as ONE indented code
    block (or a paragraph's lazy continuation) — two prose paragraphs in
    a body became ONE unit, and a scoped paragraph purified a universal
    gloss (executed on the shipped observability.md `!!! tip` and on
    docs/guides/actors.md, whose two body paragraphs the round-7
    splitter kept separate and the round-8 model folded into 145-160).
    The container split applies the RENDERER'S OWN law (`detab`: every
    line after the marker that is blank or indented >= column + 4,
    across blank lines): the body is dedented and re-parsed as markdown
    at a line offset, so each body paragraph/list/fence is its own unit
    at its true line span. A marker inside a fenced/indented code block
    or a raw HTML block is code text, never a container. Every gloss
    value is assembled at runtime from independently non-triggering
    fragments (the module's own law)."""
    scoped = "The two dispatch-invisible legs are scoped, so a damper can over-dispatch"
    gloss = "The stranded gauge: rows that " + "can never " + "be dispatched"

    # ── the executed D9-1 shape: an admonition body with two prose
    # paragraphs (blank line between) ────────────────────────────────────
    admonition = f'!!! danger "Only a reservation is strict."\n\n    {scoped}.\n\n    {gloss}.\n'
    assert _corpus_matches("virtual/admonition-prose.md", admonition) == [
        "universal-no-dispatch"
    ], (
        "the gloss paragraph must red AT ITS OWN PARAGRAPH even when the "
        "scoped paragraph shares the admonition body"
    )
    units = _corpus_units("virtual/admonition-prose.md", admonition)
    markdown_units = [unit for unit in units if unit.kind == "markdown"]
    gloss_units = [unit for unit in markdown_units if "can never" in unit.text]
    assert len(gloss_units) == 1, [unit.text for unit in markdown_units]
    assert (gloss_units[0].start_line, gloss_units[0].end_line) == (5, 5), [
        (unit.start_line, unit.end_line, unit.text[:40]) for unit in markdown_units
    ]
    scoped_units = [unit for unit in markdown_units if "dispatch-invisible" in unit.text]
    assert len(scoped_units) == 1 and scoped_units[0].start_line == 3, [
        (unit.start_line, unit.end_line, unit.text[:40]) for unit in markdown_units
    ]
    # The two body paragraphs are SEPARATE units — neither can purify
    # the other.
    assert gloss_units[0].start_line != scoped_units[0].start_line
    # ... and the corrected body (each paragraph its own words) passes.
    corrected = (
        '!!! danger "Only a reservation is strict."\n'
        "\n"
        "    The read is scoped to the two dispatch-invisible legs, so a\n"
        "    damper can over-dispatch.\n"
        "\n"
        "    The gauge's rows are scoped to the two dispatch-invisible\n"
        "    legs plus the run-scoped class.\n"
    )
    assert not _corpus_matches("virtual/admonition-prose-clean.md", corrected)

    # ── the tabbed shape (`=== "T"`) splits the same way ────────────────
    tabbed = f'=== "Tab one"\n\n    {scoped}.\n\n    {gloss}.\n'
    assert _corpus_matches("virtual/tab-prose.md", tabbed) == ["universal-no-dispatch"]
    tab_units = [
        unit
        for unit in _corpus_units("virtual/tab-prose.md", tabbed)
        if unit.kind == "markdown" and "can never" in unit.text
    ]
    assert len(tab_units) == 1 and tab_units[0].start_line == 5

    # ── the details shape (`??? note`) ──────────────────────────────────
    details = f'??? note "Collapsible"\n\n    {gloss}.\n'
    assert _corpus_matches("virtual/details-prose.md", details) == ["universal-no-dispatch"]

    # ── a marker inside a fence is CODE, never a container ──────────────
    fenced_marker = (
        '```markdown\n!!! note "an example, not a container"\n    the fence\'s own body text\n```\n'
    )
    fence_units = _corpus_units("virtual/fenced-marker.md", fenced_marker)
    assert len(fence_units) == 1, [unit.text for unit in fence_units]
    assert (fence_units[0].start_line, fence_units[0].end_line) == (1, 4), [
        (unit.start_line, unit.end_line, unit.text[:40]) for unit in fence_units
    ]

    # ... and inside an INDENTED CODE BLOCK at top level too.
    indented_marker = '    !!! note "also code"\n        not a container\n'
    indented_units = _corpus_units("virtual/indented-marker.md", indented_marker)
    assert len(indented_units) == 1 and indented_units[0].kind == "markdown", [
        unit.text for unit in indented_units
    ]

    # ── a NESTED container: the body re-parses recursively ──────────────
    nested = f'!!! note "outer"\n\n    !!! note "inner"\n        {gloss}\n'
    assert _corpus_matches("virtual/admonition-nested.md", nested) == ["universal-no-dispatch"]
    nested_units = [
        unit
        for unit in _corpus_units("virtual/admonition-nested.md", nested)
        if unit.kind == "markdown" and "can never" in unit.text
    ]
    assert len(nested_units) == 1 and nested_units[0].start_line == 4, [
        (unit.start_line, unit.end_line, unit.text[:40])
        for unit in _corpus_units("virtual/admonition-nested.md", nested)
    ]


def test_shipped_container_bodies_are_their_own_paragraphs() -> None:
    """The shipped-tree faces of D9-1: the actors.md admonition's body
    paragraphs are SEPARATE units again (the round-8 regression folded
    145-160 into one code block; the round-7 splitter had them as two),
    and the observability.md `!!! tip` body's fence is its own unit at
    its own true span. Read-only over the real files."""
    raw = (REPO_ROOT / "docs/guides/actors.md").read_text(encoding="utf-8")
    units = [
        unit for unit in _markdown_units("docs/guides/actors.md", raw) if unit.kind == "markdown"
    ]
    identity = [
        unit for unit in units if unit.text.startswith("**`identity_key` does not fix this.**")
    ]
    singleton = [
        unit
        for unit in units
        if unit.text.startswith("**`singleton=True` is a different guarantee")
    ]
    assert len(identity) == 1 and len(singleton) == 1, [
        (unit.start_line, unit.end_line, unit.text[:60]) for unit in units
    ]
    # The two paragraphs are separate units at their own true spans —
    # neither contains the other (the differential against the round-8
    # fold is preserved in the round-9 evidence logs).
    assert identity[0].end_line < singleton[0].start_line, (
        f"the admonition body's paragraphs folded: identity "
        f"{identity[0].start_line}-{identity[0].end_line}, singleton "
        f"{singleton[0].start_line}-{singleton[0].end_line}"
    )

    obs_raw = (REPO_ROOT / "docs/guides/observability.md").read_text(encoding="utf-8")
    obs_units = [
        unit
        for unit in _markdown_units("docs/guides/observability.md", obs_raw)
        if unit.kind == "markdown"
    ]
    tip_units = [
        unit for unit in obs_units if unit.text.startswith('!!! tip "Which job is starving?')
    ]
    assert len(tip_units) == 1, [(u.start_line, u.end_line) for u in obs_units]
    # The admonition's body fence is its OWN unit — never merged with the
    # marker line above it.
    fence_units = [unit for unit in obs_units if "SELECT" in unit.text and "```" in unit.text]
    assert len(fence_units) == 1 and fence_units[0].start_line > tip_units[0].start_line, [
        (u.start_line, u.end_line, u.text[:60]) for u in obs_units
    ]


def test_md_html_block_siblings_are_their_own_units() -> None:
    """Round-9 D9-2: a `.md` file's `html_block` gets the same element/
    comment model the `.html` surface gives — ONE html_block token
    folded sibling `<tr>` rows and sibling wrapped `<!-- -->` comments
    into one unit (latent on the shipped docs, which carry no raw HTML
    table; the build enables `md_in_html`). The gloss sibling reds ALONE
    at its own element unit. Every gloss value is assembled at runtime
    from independently non-triggering fragments (the module's own
    law)."""
    scoped_row = "<tr><td>The two dispatch-invisible legs are scoped</td></tr>"
    gloss_row = (
        "<tr><td>"
        + frag_reasons
        + "</td><td>The stranded gauge: rows that "
        + "can never "
        + "dispatch.</td></tr>"
    )
    table = "<table>\n" + scoped_row + "\n" + gloss_row + "\n</table>\n"
    assert _corpus_matches("virtual/md-table.md", table) == ["universal-no-dispatch"], (
        "the gloss row must red EVEN WHEN the sibling row carries the "
        "scoping marker — a .md html_block's sibling elements are their "
        "own units"
    )
    row_units = [
        unit
        for unit in _corpus_units("virtual/md-table.md", table)
        if unit.kind == "html-element" and unit.text.startswith("<tr>")
    ]
    assert len(row_units) == 2, [unit.text for unit in row_units]

    scoped_comment = "<!-- the scoped read covers the two dispatch-invisible legs -->"
    gloss_comment = "<!-- the stranded gauge: rows that " + "can never " + "be dispatched -->"
    div = "<div>\n" + scoped_comment + "\n" + gloss_comment + "\n</div>\n"
    assert _corpus_matches("virtual/md-div.md", div) == ["universal-no-dispatch"], (
        "the wrapped gloss comment must red EVEN WHEN the sibling comment "
        "carries the scoping marker"
    )
    comment_units = [
        unit for unit in _corpus_units("virtual/md-div.md", div) if unit.kind == "html-comment"
    ]
    assert len(comment_units) == 2, [unit.text for unit in comment_units]


def test_quoted_fence_comment_lines_are_their_own_units() -> None:
    """Round-9 D9-3: a fence inside a blockquote — the raw lines carry
    the `> ` prefix, `.strip()` never removes it, so the per-line
    comment refinement was DEAD inside blockquotes and quoting a fence
    re-opened round-7's adjacent-comment defeat (two shipped files
    carry quoted fences: sso.md's dockerfile fence and
    managed-identities.md's python fence). The comment units now come
    from the parser's DE-PREFIXED `token.content` mapped back through
    the token's line map — quoting cannot defeat them. Every gloss
    value is assembled at runtime from independently non-triggering
    fragments (the module's own law)."""
    gloss_comment = "-- the stranded gauge: rows that " + "can never " + "be dispatched"
    scoped_comment = "-- scoped to the two dispatch-invisible legs only"
    quoted = "> ```sql\n> " + gloss_comment + "\n> " + scoped_comment + "\n> SELECT 1;\n> ```\n"
    assert _corpus_matches("virtual/quoted-fence.md", quoted) == ["universal-no-dispatch"], (
        "the quoted fence's gloss comment must red EVEN WHEN the adjacent "
        "comment line carries the scoping marker — the blockquote prefix "
        "cannot defeat the comment units"
    )
    comment_units = [
        unit
        for unit in _corpus_units("virtual/quoted-fence.md", quoted)
        if unit.text.lstrip("> ").strip().startswith("--")
    ]
    assert len(comment_units) == 2, [unit.text for unit in comment_units]
    gloss_units = [unit for unit in comment_units if "can never" in unit.text]
    assert len(gloss_units) == 1 and (gloss_units[0].start_line, gloss_units[0].end_line) == (
        2,
        2,
    ), [(unit.start_line, unit.end_line, unit.text[:50]) for unit in comment_units]
    # The shipped quoted fences (sso.md's dockerfile fence,
    # managed-identities.md's python fence): a quoted fence is modeled at
    # its own true span — the prefix mapping cannot crash or lose it —
    # and its comment lines are their own units (the dockerfile's
    # trailing `#` comment included; the round-10 F10-3 model).
    sso_raw = (REPO_ROOT / "docs/guides/sso.md").read_text(encoding="utf-8")
    sso_units = list(_markdown_units("docs/guides/sso.md", sso_raw))
    dockerfile_fences = [
        unit
        for unit in sso_units
        if unit.kind == "markdown"
        and unit.text.lstrip("> ").startswith("```dockerfile")
        and "FROM taskq-worker" in unit.text
    ]
    assert (
        len(dockerfile_fences) == 1
        and dockerfile_fences[0].start_line < dockerfile_fences[0].end_line
    ), [(u.start_line, u.end_line, u.text[:60]) for u in sso_units if u.kind == "markdown"]
    sso_comment_units = [
        unit
        for unit in sso_units
        if unit.kind == "markdown"
        and unit.start_line == unit.end_line
        and _TRAILING_COMMENT_START.search(unit.text) is not None
        and "Dockerfile" in unit.text
    ]
    assert (
        len(sso_comment_units) == 1
        and sso_comment_units[0].start_line > dockerfile_fences[0].start_line
    ), [(u.start_line, u.end_line, u.text[:60]) for u in sso_comment_units]
    managed_raw = (REPO_ROOT / "docs/guides/managed-identities.md").read_text(encoding="utf-8")
    managed_units = _markdown_units("docs/guides/managed-identities.md", managed_raw)
    redis_fences = [
        unit
        for unit in managed_units
        if unit.kind == "markdown"
        and unit.text.lstrip("> ").startswith("```")
        and "ExponentialBackoff" in unit.text
    ]
    assert len(redis_fences) == 1 and redis_fences[0].start_line < redis_fences[0].end_line, [
        (u.start_line, u.end_line, u.text[:60]) for u in managed_units if u.kind == "markdown"
    ]


def test_fence_block_comments_are_their_own_units() -> None:
    """Round-9 D9-4: `_FENCE_COMMENT_PREFIXES` only knew line comments —
    a `/* ... */` or `<!-- ... -->` block comment inside a fence got no
    unit and could be purified by a sibling scoped line comment. A block
    comment is ONE statement: the whole statement is its own unit (a
    multi-line comment stays the one unit it is). Every gloss value is
    assembled at runtime from independently non-triggering fragments
    (the module's own law)."""
    scoped_comment = "-- scoped to the two dispatch-invisible legs only"
    gloss = "the stranded gauge: rows that " + "can never " + "dispatch"
    fence = "```sql\n" + scoped_comment + "\n/* " + gloss + " */\nSELECT 1;\n```\n"
    assert _corpus_matches("virtual/fence-block-comment.md", fence) == ["universal-no-dispatch"], (
        "the block-comment gloss must red EVEN WHEN the sibling line "
        "comment carries the scoping marker — a block comment is its own "
        "unit"
    )
    block_units = [
        unit
        for unit in _corpus_units("virtual/fence-block-comment.md", fence)
        if unit.text.strip().startswith("/*")
    ]
    assert len(block_units) == 1 and (block_units[0].start_line, block_units[0].end_line) == (
        3,
        3,
    ), [(unit.start_line, unit.end_line, unit.text[:60]) for unit in block_units]

    # A multi-line block comment stays ONE unit and still reds.
    multiline = "```sql\n/* " + gloss + ",\n   wrapped across two lines */\nSELECT 1;\n```\n"
    multiline_units = [
        unit
        for unit in _corpus_units("virtual/fence-block-multiline.md", multiline)
        if unit.text.strip().startswith("/*")
    ]
    assert len(multiline_units) == 1, [unit.text for unit in multiline_units]
    assert (multiline_units[0].start_line, multiline_units[0].end_line) == (2, 3)
    assert _corpus_matches("virtual/fence-block-multiline.md", multiline), (
        "a multi-line block comment carrying the gloss reds as its own unit"
    )

    # An HTML block comment inside an html fence too.
    html_fence = (
        "```html\n<!-- " + gloss + " -->\n<p>scoped to the two dispatch-invisible legs</p>\n```\n"
    )
    assert _corpus_matches("virtual/fence-html-comment.md", html_fence), (
        "a `<!-- -->` gloss inside a fence reds as its own unit"
    )


def test_trailing_and_other_prefix_fence_comments_are_their_own_units() -> None:
    """Round-10 F10-3: `_code_comment_units` gave a unit only to a
    comment line the comment marker STARTS — a TRAILING comment
    (`SELECT 1;  -- <gloss>`) had no unit of its own, so a scoped
    LEADING comment plus a trailing gloss stayed green (the fence's own
    coarse unit carried both, and the sibling's marker purified the
    gloss); and the `%`/`;` line-comment prefixes (LaTeX/Matlab, Lisp)
    were not recognised at all. A trailing comment's unit is the
    comment's own line; an over-taken marker (a `//` inside a URL) can
    only add a unit, never hide one. The round-11 F11-1 correction: the
    marker needs NO preceding whitespace (`SELECT 1;-- <gloss>` is a
    comment statement too). Every gloss value is assembled at
    runtime from independently non-triggering fragments (the module's
    own law)."""
    scoped_comment = "-- scoped to the two dispatch-invisible legs only"
    gloss = "the stranded gauge: rows that " + "can never " + "dispatch"
    # The executed defeat: a scoped LEADING comment plus a TRAILING gloss.
    fence = "```sql\n" + scoped_comment + "\nSELECT 1;  -- " + gloss + "\n```\n"
    assert _corpus_matches("virtual/fence-trailing-gloss.md", fence) == ["universal-no-dispatch"], (
        "the trailing gloss must red EVEN WHEN the leading comment carries the scoping marker"
    )
    comment_units = [
        unit
        for unit in _corpus_units("virtual/fence-trailing-gloss.md", fence)
        if unit.kind == "markdown"
        and unit.start_line == unit.end_line
        and (
            unit.text.strip().startswith(_FENCE_COMMENT_PREFIXES)
            or _TRAILING_COMMENT_START.search(unit.text) is not None
        )
    ]
    assert len(comment_units) == 2, [unit.text for unit in comment_units]
    trailing = [unit for unit in comment_units if "SELECT" in unit.text]
    assert len(trailing) == 1 and (trailing[0].start_line, trailing[0].end_line) == (3, 3), [
        (unit.start_line, unit.end_line, unit.text[:50]) for unit in comment_units
    ]
    # ... and the corrected fence (the trailing comment scoped in its
    # own words) passes.
    corrected = (
        "```sql\n"
        + scoped_comment
        + "\nSELECT 1;  -- scoped to the two dispatch-invisible legs only\n```\n"
    )
    assert not _corpus_matches("virtual/fence-trailing-clean.md", corrected)
    # The `%` and `;` line-comment prefixes: a gloss comment line in
    # each REDS ALONE, and a scoped sibling never purifies it.
    for prefix in ("%", ";"):
        gloss_fence = (
            f"```text\n{prefix} " + gloss + "\n"
            f"{prefix} scoped to the two dispatch-invisible legs only\nSELECT 1;\n```\n"
        )
        assert _corpus_matches(f"virtual/fence-{prefix}-gloss.md", gloss_fence) == [
            "universal-no-dispatch"
        ], f"a '{prefix}'-comment gloss line must red EVEN WHEN its sibling carries the marker"
        units = [
            unit
            for unit in _corpus_units(f"virtual/fence-{prefix}-gloss.md", gloss_fence)
            if unit.text.strip().startswith(prefix)
        ]
        assert len(units) == 2, [unit.text[:50] for unit in units]

    # ── the round-11 F11-1 defeats ──────────────────────────────────────
    # A comment marker with NO whitespace before it is still a comment
    # start: `SELECT 1;-- <gloss>` is valid SQL (`--` begins a line
    # comment with no preceding-whitespace requirement), and the same
    # holds for `#` in Python, `//` in the C family, `%` in LaTeX and
    # `;` in Lisp. Each no-space trailing form gets a unit of its own
    # whose own sentence carries no marker, so it REDS even when a
    # scoped sibling comment precedes it; the corrected no-space
    # trailing comment passes.
    for code, marker in (
        ("SELECT 1;", "--"),
        ("x = 1;", "%"),
        ("echo done;", "#"),
        ("int x = 1;", "//"),
    ):
        defeat = "```text\n" + scoped_comment + "\n" + code + marker + " " + gloss + "\n```\n"
        assert _corpus_matches("virtual/fence-nospace-trailing.md", defeat) == [
            "universal-no-dispatch"
        ], (
            f"a no-whitespace trailing '{marker}' gloss must red EVEN WHEN the scoped sibling precedes it"
        )
        corrected = (
            "```text\n" + code + marker + " scoped to the two dispatch-invisible legs only\n```\n"
        )
        assert not _corpus_matches("virtual/fence-nospace-clean.md", corrected), (
            f"a corrected no-whitespace trailing '{marker}' comment must pass"
        )


def test_scoping_requires_a_genuine_scope() -> None:
    """Round-9 D9-6 + round-10 F10-4: the scoping test was a naive
    substring match over `_SCOPED_MARKERS`, so a NEGATION or a CONTRAST
    waived rule 2 — "…are **not** dispatch-invisible: they can never
    dispatch" carried the marker "not dispatch-invisible" and passed;
    "not only the two dispatch-invisible legs" carried "only the two";
    "…can never dispatch; jobs on a served queue can dispatch
    normally" carried "can dispatch"; and "two dispatch" matched "two
    dispatchers". The predicate is now a real scope check:
    word-bounded markers that NAME the dispatch-invisible legs (so "The
    two dispatch queues are `default` and `bulk`" is not a scope), a
    negator anywhere in the marker's own SENTENCE kills it (clause-
    bounded — the old 20-character window let "not in any sense at all
    what anyone would call dispatch-invisible" pass), a scope sentence
    waives a UNIVERSAL occurrence only within its own sentence (the
    third-leg naming may rescue a counter-claim only inside the third
    leg's own definition sentence — the launder that names the third
    leg, states the scope, then adds a whole-population universal
    sentence REDS), and the truthful SUBJECT-SCOPED form (one reason's
    own rows as the claim's own subject) is green. Every corpus value
    is assembled at runtime from independently non-triggering fragments
    (the module's own law)."""
    # The four executed negation/contrast defeats — each REDS.
    negated_marker = (
        "The stranded gauge's rows are "
        + "not "
        + "dispatch-"
        + "invisible: they "
        + "can never "
        + "dispatch."
    )
    assert _corpus_matches("virtual/scope-negated.md", negated_marker) == [
        "universal-no-dispatch"
    ], "a negated marker is not a scope"
    not_only = (
        "The stranded detector's pending/scheduled rows are "
        + "not only the two "
        + "dispatch-invisible legs — these rows "
        + "can never "
        + "dispatch."
    )
    assert _corpus_matches("virtual/scope-not-only.md", not_only) == ["universal-no-dispatch"], (
        "a 'not only' contrast is not a scope"
    )
    contrast = (
        "The stranded gauge: rows on an unserved queue "
        + "can never "
        + "dispatch; jobs on a served queue "
        + "can dispatch"
        + " normally."
    )
    assert _corpus_matches("virtual/scope-contrast.md", contrast) == [
        "closed-enumeration",
        "universal-no-dispatch",
    ], "a positive counter-claim contradicts the universal claim instead of scoping it"
    two_dispatchers = (
        "The two dispatchers share the gauge; the stranded detector's rows "
        + "can never "
        + "dispatch."
    )
    assert _corpus_matches("virtual/scope-two-dispatchers.md", two_dispatchers) == [
        "universal-no-dispatch"
    ], "'two dispatch' must not match 'two dispatchers'"

    # ── the round-10 F10-4 defeats ──────────────────────────────────────
    # (a) "two dispatch" is not a scope marker: naming the dispatcher
    # pools is not scoping the legs — the universal gloss REDS.
    dispatch_queues = (
        "The doctor prints the count. The stranded rows "
        + "can never "
        + "dispatch. The two dispatch queues are `default` and `bulk`."
    )
    assert _corpus_matches("virtual/scope-dispatch-queues.md", dispatch_queues) == [
        "universal-no-dispatch"
    ], "naming the two dispatch QUEUES is not a scope"
    # (b) the negator outside the old 20-character window: clause-bounded
    # negation kills the marker — REDS.
    far_negator = (
        "The stranded rows are not in any sense at all what anyone would "
        + "call dispatch-invisible: they "
        + "can never "
        + "dispatch."
    )
    assert _corpus_matches("virtual/scope-far-negator.md", far_negator) == [
        "universal-no-dispatch"
    ], "a negator in the marker's own sentence is not out of reach"
    # (c) the third-leg WAIVER cannot launder: naming the third leg and
    # stating the scope does not waive a whole-population universal
    # sentence — REDS.
    launder = (
        "The third leg is workflow_root_missing. The two dispatch-invisible "
        "legs are scoped. All stranded rows " + "can never " + "dispatch."
    )
    assert _corpus_matches("virtual/scope-third-leg-launder.md", launder) == [
        "universal-no-dispatch"
    ], "the third leg's name waives only its own definition sentence's counter-claim"
    # (d) the truthful SUBJECT-SCOPED form is GREEN (the round-10 false
    # positive): the claim's own subject is one reason's own rows.
    subject_scoped = "The stranded gauge: " + frag_reasons + " rows can never dispatch."
    assert not _corpus_matches("virtual/scope-subject-scoped.md", subject_scoped), (
        "one reason's own rows as the claim's own subject is that reason's true meaning"
    )
    # ... the population subject between the label and the variant never
    # waives (the table-cell shape), and the anaphoric two-sentence form
    # binds back to the unit's subject — both REDS.
    population_between = "| " + frag_reasons + " | these rows can never dispatch |"
    assert _corpus_matches("virtual/scope-population-between.md", population_between) == [
        "closed-enumeration",
        "universal-no-dispatch",
    ], "a population subject between the label and the variant is the whole population"
    anaphoric = "The gauge's rows are stranded. They " + "can never " + "dispatch."
    assert _corpus_matches("virtual/scope-anaphoric.md", anaphoric) == ["universal-no-dispatch"], (
        "a pronoun subject binds back to the unit's own subject"
    )
    # ... and the anaphoric refusal is POSITION-INDEPENDENT (round-12
    # D12-2): the old check anchored the pronoun to the head's end, so a
    # mid-head pronoun ("They themselves …", "They in turn …") hid the
    # anaphora. Both shapes REDS.
    pronoun_mid = "The stranded detector. They themselves " + "can never " + "dispatch."
    assert _corpus_matches("virtual/scope-pronoun-mid.md", pronoun_mid) == [
        "universal-no-dispatch"
    ], "a mid-head pronoun still binds back to the unit's own subject"
    pronoun_phrase = "The stranded detector. They in turn " + "can never " + "dispatch."
    assert _corpus_matches("virtual/scope-pronoun-phrase.md", pronoun_phrase) == [
        "universal-no-dispatch"
    ], "a pronoun before a trailing phrase still binds back"
    # ── the round-13 N13-1 defeats: the FULL anaphoric paradigm ─────────
    # The possessive/reflexive/plural pronoun forms bind back to the
    # unit's subject exactly like they/it/them — the gloss's own
    # sentence has no detector mention, so only the pronoun refusal
    # stops the hide. Each shape REDS. The set is declared in
    # `_PRONOUN_SUBJECT_RE` and residual 7; every new form has its own
    # control here.
    for name, shape in (
        ("their", "The stranded detector's rows wait. Their rows " + frag_forever + "."),
        ("its", "The stranded detector's rows wait. Its rows " + frag_forever + "."),
        ("itself", "The stranded detector's rows wait. The gauge itself " + frag_forever + "."),
        ("theirs", "The stranded detector's rows wait. Theirs " + frag_forever + "."),
        ("we", "The stranded detector's rows wait. We never " + "dispatched the backlog."),
        ("us", "The stranded detector's rows wait. For us the rows stay " + frag_forever + "."),
        ("our", "The stranded detector's rows wait. Our rows " + frag_forever + "."),
        ("ours", "The stranded detector's rows wait. Ours " + frag_forever + "."),
        (
            "ourselves",
            "The stranded detector's rows wait. We ourselves never " + "dispatched the backlog.",
        ),
        (
            "themselves",
            "The stranded detector's rows wait. The rows themselves " + frag_forever + ".",
        ),
    ):
        assert _corpus_matches(f"virtual/pronoun-{name}.md", shape) == ["universal-no-dispatch"], (
            f"the anaphoric pronoun {name!r} still binds back to the unit's own subject"
        )
    # ── the round-11 F11-2 defeats ──────────────────────────────────────
    # A population subject EARLIER in the sentence is the claim's
    # subject — the label+noun tail after it does not make the claim
    # one reason's own meaning. Each shape REDS.
    population_earlier = (
        "The stranded gauge. All stranded entries like "
        + frag_reasons
        + " rows "
        + "can never "
        + "dispatch."
    )
    assert _corpus_matches("virtual/scope-population-earlier.md", population_earlier) == [
        "closed-enumeration",
        "universal-no-dispatch",
    ], "a population subject earlier in the sentence defeats the label waiver"
    population_unlike = (
        "These stranded entries unlike " + frag_reasons + " rows " + "can never " + "dispatch."
    )
    assert _corpus_matches("virtual/scope-population-unlike.md", population_unlike) == [
        "closed-enumeration",
        "universal-no-dispatch",
    ], "'unlike' does not turn the population subject into one reason's rows"
    population_not_like = (
        "All stranded entries not like " + frag_reasons + " rows " + "can never " + "dispatch."
    )
    assert _corpus_matches("virtual/scope-population-not-like.md", population_not_like) == [
        "closed-enumeration",
        "universal-no-dispatch",
    ], "'not like' does not defeat the population subject"
    population_unserved = (
        "All stranded entries like " + frag_unserved + " rows " + "can never " + "dispatch."
    )
    assert _corpus_matches("virtual/scope-population-unserved.md", population_unserved) == [
        "closed-enumeration",
        "universal-no-dispatch",
    ], "the unserved label does not rescue a population subject"
    # ... and a foreign restricted subject with no detector mention in
    # its own sentence is that sentence's own subject — GREEN.
    foreign = (
        "An actor that can never be dispatched because it has no "
        "registered stub/config is a strictly worse failure mode than one "
        "that dispatches and then immediately errors."
    )
    assert not _corpus_matches("virtual/scope-foreign-subject.md", foreign), (
        "a foreign restricted subject is not a gloss on the gauge"
    )

    # ── the round-12 D12-1 defeats: the family's OWN canonical spellings
    # are detector mentions ──────────────────────────────────────────────
    # The old sentence-mention list ended at bare `stranded`, so the
    # `(?![\w-])` lookahead rejected the hyphen and the lookbehind
    # rejected the underscore: a universal gloss in the family's own
    # canonical spelling (`stranded-jobs`, the `taskq_jobs_stranded`
    # metric) was treated as a foreign restricted subject and waived.
    # Every spelling REDS, the space-spelled control included.
    for name, shape in (
        ("space", "The stranded jobs detector: " + frag_universal + "."),
        ("hyphen-detector", "The stranded-jobs detector: " + frag_universal + "."),
        ("hyphen-gauge", "The stranded-jobs gauge: " + frag_universal + "."),
        ("metric-gauge", "The taskq_jobs_stranded gauge: " + frag_universal + "."),
        ("table-row", "| `stranded-jobs detector` | " + frag_universal + " |"),
    ):
        assert _corpus_matches(f"virtual/mention-{name}.md", shape) == ["universal-no-dispatch"], (
            f"the family's own {name} spelling is a detector mention"
        )

    # ── the round-12 D12-3 pins: the label-bound parenthetical
    # exception's ACCEPTED reach, declared in residual 4 ────────────────
    # The exception is PAREN-BOUND, not subject-verified: a sentence
    # whose subject is the population and whose label is only mentioned
    # hides the same way. These three shapes PIN that accepted reach as
    # the NARROWING tripwire (round-13 N13-5 corrected the old claim):
    # narrowing the exception reds them and false-reds exactly the two
    # shipped alert-description spans. WIDENING leaves them green —
    # executed: a drop-the-between-text-check widening mutant passes
    # every corpus pin and the tree scan (0 failures) — so the widening
    # direction is pinned separately by the inside-parenthetical
    # control below.
    for name, shape in (
        (
            "unlike",
            "All stranded entries, unlike " + frag_unserved + " (" + frag_universal + ".",
        ),
        (
            "defect",
            "All stranded entries share the "
            + frag_unserved
            + " defect (so they are "
            + frag_universal
            + ".",
        ),
        (
            "never-dispatched",
            "All stranded entries, unlike " + frag_unserved + " (" + frag_forever + ".",
        ),
    ):
        assert not _corpus_matches(f"virtual/label-bound-{name}.md", shape), (
            f"the D12-3 accepted pin ({name}) changed — the parenthetical "
            "exception's reach moved; re-declare residual 4 and re-review "
            "the shipped alert descriptions' elliptical form"
        )

    # ── the round-13 N13-5 widening pin: the inside-parenthetical gloss
    # REDS ──────────────────────────────────────────────────────────────
    # A population subject INSIDE the label's own unclosed parenthetical
    # defeats the exception (the between-text check runs first —
    # `_label_bound`'s declared law). This is the WIDENING direction's
    # only tripwire: a mutant dropping the between-text check passes
    # every corpus pin above and the tree scan (0 failures) while
    # flipping this shape green — the assertion below is what reds it.
    inside_paren_population = (
        "All stranded entries, unlike " + frag_unserved + " (these rows " + frag_universal + "."
    )
    assert _corpus_matches(
        "virtual/label-bound-inside-paren-population.md", inside_paren_population
    ) == ["universal-no-dispatch"], (
        "a population subject inside the label's own parenthetical must "
        "defeat the exception — the widening tripwire (round-13 N13-5); "
        "if this greens, the between-text population check was dropped "
        "or weakened and the exception's documented reach grew"
    )

    # ... but the third leg's OWN definition (the shipped prose's shape:
    # the unit names workflow_root_missing and says its row CAN dispatch
    # when served) is the reason set's own statement — green.
    third_leg_definition = (
        '`reason="workflow_root_missing"` is a different class of fault: '
        "the two dispatch-invisible reasons never touch such rows. Such a "
        "row " + "can dispatch" + " and be bounded-consumed when its actor config and effective "
        "routing queue are otherwise served."
    )
    assert not _corpus_matches("virtual/scope-third-leg.md", third_leg_definition), (
        "the third leg's own can-dispatch definition must not false-red"
    )

    # The genuine scoped forms stay green.
    genuine = (
        "The gauge's rows are scoped to "
        + "the two dispatch-invisible legs"
        + " ("
        + frag_reasons
        + " and "
        + frag_unserved
        + ") only."
    )
    assert not _corpus_matches("virtual/scope-genuine.md", genuine), (
        "the genuine scoped form must not false-red"
    )
    may_dispatch = (
        "The stranded detector's reason set: "
        + frag_reasons
        + " (the row is invisible to every dispatch round), "
        + frag_unserved
        + " (likewise dispatch-invisible), and workflow_root_missing (the "
        "row may dispatch and run when served)."
    )
    assert not _corpus_matches("virtual/scope-may-dispatch.md", may_dispatch), (
        "the third leg's own may-dispatch definition is a genuine scope"
    )
    # The scope binding across the colon and the parenthetical's
    # semicolon (the shipped scope sentences' own shapes) stays green:
    # a variant in the scope's own sentence-bound span is waived.
    colon_bound = (
        "These two reasons are dispatch-invisible: neither dispatch nor "
        "the deadline sweep will ever touch such rows."
    )
    assert not _corpus_matches("virtual/scope-colon-bound.md", colon_bound), (
        "the scope binds across its own colon — the shipped runbook sentence"
    )
