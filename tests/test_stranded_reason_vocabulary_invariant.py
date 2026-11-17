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
enumeration with a maintained scanner:

* **semantic local units** — Markdown STRUCTURAL siblings (each tight
  list item at its own marker, each blockquote paragraph, each fenced
  block with each fenced line-comment line, each table row; a wrapped
  paragraph, a list item's own continuation lines and a single fenced
  block's content stay the one statement they are), YAML scalar
  values (parsed and folded), YAML comment lines, HTML element runs
  (each `<tr>` row / block element; a wrapped element stays one unit),
  HTML `<!-- -->` and Jinja/Tera `{# #}` comment statements, AST
  docstrings, contiguous comment blocks, and `Field(description=...)`
  strings — never "a label occurs somewhere in the file", and never a
  blank-line block folding sibling elements/items/quotes/comments into
  one unit (a scoped sibling must not purify a gloss);
* **four detection rules** — (1) the closed reason-set enumeration, (2)
  the universal no-dispatch/no-touch claim, (3) the whole-detector
  parity/mirror claim, (4) the structural closed enumeration (every
  `Literal[...]`/tuple/list/set carrying the stranded reason labels);
* **fail-closed parsing** — an unreadable file or a Python file that
  fails `ast.parse` is RED, never skipped;
* **a span-digest allowlist** — `path + normalized-span digest +
  one-line reason`; an unlisted match is red; a vanished required path
  or span is red (deleting text cannot silently erase an exception);
* **self-referential fixtures constructed at runtime** — the negative
  corpus is built from independently non-triggering fragments, so no
  stored counterexample can exempt or poison the scan.

This test is the SOLE completeness authority for this class. Per-site
red-first evidence (every corrected site's own old wording reverts and
reds this scanner) lives in the correction report; a new occurrence
anywhere in the scanned tree reds THIS test without waiting for another
review to find it."""

from __future__ import annotations

import ast
import bisect
import hashlib
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Final

import pytest

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
        "426ccb43bc4ca1c9e8dfa5f819c1570e36e86d9ce007288605e5d7c3cc24a547",
        "src/taskq/cli.py",
        "the doctor's scoped mirror comment: the two dispatch-invisible legs named AS a subset",
    ),
    (
        "7b7d2746db5efe91544f5f7e2e48fe5d06431c7bd730d350418bc3c2254bf2cf",
        "src/taskq/cli.py",
        "the doctor's scoped mirror comment: the two dispatch-invisible legs named AS a subset",
    ),
    (
        "613caf5bb80e71cffbc7b8efcc1a58686ce43e0345a231cd6475a8d7d75faa97",
        "src/taskq/settings.py",
        "the ADMIN_WORKER_LIVENESS description is the liveness window's own subject (which arms read the liveness edge), not a reason-set enumeration",
    ),
    (
        "6c980c36d6e92dcc6b9b254f9db31546d3119affaf4c41f271266d0113484986",
        "src/taskq/web/templates/_partials/queue_table.html",
        "the queue badge tooltip's own sanctioned two-leg text (layer-6 table B: the tooltip exactly describes the two legs its SQL counts)",
    ),
    (
        "c842b501aea0bfaf6804636b11729bb9cd432f4344c82095d225fa6d63d4df1f",
        "src/taskq/web/templates/_partials/queue_table.html",
        "the SAME sanctioned two-leg tooltip, as its own `<span>` element unit "
        "carries it (the round-6 M-1 element units re-derived the digest; a "
        "wording touch still re-reviews the span)",
    ),
    (
        "afa336cda95c34edb17f682d4800764144a449a63e0a331897d896c6d7232cf1",
        "docs/guides/admin-ui.md",
        "the Live Workers column's own subject is the liveness read's agreement across surfaces, not a reason enumeration",
    ),
    (
        "cb4f430adf27cd1a6369112810469205a9779f187ecd2752941998609ffe1eae",
        "docs/guides/observability.md",
        "the alert inventory list item (the shipped rules' names), not a reason enumeration",
    ),
    (
        "a74123c4e98e61863450a38f5b96a18c825c305725d0ab70b5e18a43aec96785",
        "docs/guides/configuration.md",
        "the liveness env row's own subject (the unserved arm's liveness dependency, named instrumentally) — not a reason-set enumeration",
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
        "0760aec62348f6ebe393eaca4ddbb933c6297a5b5f06ce9375ac2e2ccc7e521e",
        "docs/guides/runbooks.md",
        "the TaskQQueueUnserved section's own remedy step — the sibling alert's subject",
    ),
    (
        "eba6473c5b0c4f05523bc97b2afe7a432176aa5b44e93bbbc654561a412dd7b0",
        "docs/guides/testing.md",
        "the fake runner's run-until-drained refusal is about its own registration/stub mechanism, not the leader gauge",
    ),
    (
        "20493d11e1625e96eca5973ee5136980574c9fb18d2358cf81238d859564e90e",
        "docs/guides/troubleshooting.md",
        "actor/queue diagnostic topics, subject-scoped",
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
        "a760c9fd925e0b3f12ecb88dfed39f74551ddc0c5846aeef6b95ce467f0189e3",
        "tests/attack_wf_engine_windows.py",
        "the attack docstring's own subject (the crash-window wire shapes; the never-dispatched mention is the reclaimed-row subject)",
    ),
    (
        "8e7a46427c8ca1fb5692e8515e7e60128d0eca72d5e9a013330c8af26ca76369",
        "tests/test_cli_doctor.py",
        "the doctor's own stated purpose quotes — the two-leg subject's true never-dispatches gloss",
    ),
    (
        "b8b56507674d9401f3512e278fef2a41c2ae86b6927b52246c68d9d50532358d",
        "tests/test_cli_doctor.py",
        "the doctor's own stated purpose quotes — the two-leg subject's true never-dispatches gloss",
    ),
    (
        "baa47ee27942bfc46fbe5acf46d0f74bdf7668c26ab0ad135f2e69ac71120e15",
        "tests/test_ops_liveness_boundary_contract.py",
        "the liveness-boundary contract's own subject (where the liveness edge sits), not a reason enumeration",
    ),
    (
        "64753e245ef4e48e4df1540cbee5a54f22b43cd12abaf4422b2e5b7ed75cfad4",
        "tests/test_prometheus_metrics_review.py",
        "the sanctioned two-label subset regression fixture (layer-6 table A: kept as a subset control with separate third-reason coverage)",
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
    (
        "92620b0d08dd0f15b25e36b084c243b5aeea2549497c41913b731070576dd5ba",
        "docs/guides/troubleshooting.md",
        "the stranded-jobs cause row's own subject-scoped text (the no-actor-config cause as the troubleshooting table states it)",
    ),
    (
        "20493d11e1625e96eca5973ee5136980574c9fb18d2358cf81238d859564e90e",
        "docs/guides/troubleshooting.md",
        "the stranded-jobs cause row's own subject-scoped text (the no-actor-config cause as the troubleshooting table states it)",
    ),
    (
        "415e5bac0cf540817600e9f34c21f3a4b249cc51339738d795f488609e62d136",
        "tests/test_reclaim_indefinite_deadline_handoff_attack.py",
        "the reclaim/deadline handoff attack's own subject (the schedule_to_close window's mechanics), not the gauge's reason set",
    ),
)

# ── the match units ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class Unit:
    """One semantic local unit: the text, its location, its source span."""

    path: str
    kind: str
    start_line: int
    end_line: int
    text: str

    @property
    def span_digest(self) -> str:
        normalized = "\n".join(line.rstrip() for line in self.text.splitlines())
        return hashlib.sha256(normalized.encode()).hexdigest()


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
_SCOPED_MARKERS = (
    "dispatch-invisible",
    "dispatch invisible",
    "two dispatch",
    "two dispatch-invisible legs",
    "only the two",
    "scoped to",
    "may dispatch",
    "can dispatch",
    "may be dispatched",
    "not dispatch-invisible",
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


#: An UNESCAPED pipe — GFM treats ``\|`` as a literal pipe, never a cell
#: boundary, so the row detector splits on this pattern only.
_UNESCAPED_PIPE: Final = re.compile(r"(?<!\\)\|")

#: One delimiter-row cell: optional colons around one or more dashes.
_DASH_CELL: Final = re.compile(r":?-+:?")


def _has_unescaped_pipe(stripped: str) -> bool:
    return _UNESCAPED_PIPE.search(stripped) is not None


def _is_table_separator(stripped: str) -> bool:
    """A GFM delimiter row: dash cells (with optional colons) separated by
    unescaped pipes, with or without the outer pipes themselves."""
    if not _has_unescaped_pipe(stripped):
        # A pipe-less dash run is a thematic break or a setext underline,
        # never a table's delimiter row (a single-column table's header
        # could not carry the pipe a row unit requires anyway).
        return False
    cells = _UNESCAPED_PIPE.split(stripped)
    seen_dash = False
    for position, cell in enumerate(cells):
        text = cell.strip()
        if text == "" and position in (0, len(cells) - 1):
            continue  # an omitted outer pipe's empty cell
        if _DASH_CELL.fullmatch(text) is None:
            return False
        seen_dash = True
    return seen_dash


#: A list item marker: `-`, `*`, `+`, `N.` or `N)` followed by item
#: whitespace. The match's end offset is the item's content column (the
#: minimum indentation of the item's own continuation lines).
_LIST_MARKER: Final = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")

#: A blockquote prefix: one or more `>` markers, each optionally followed
#: by one space, after up to three leading spaces.
_QUOTE_PREFIX: Final = re.compile(r"^ {0,3}(?:>\s?)+")

#: A fenced-block delimiter: up to three leading spaces, optionally
#: preceded by blockquote markers (a fence inside a blockquote), then
#: three or more backticks or tildes. The OPENING fence may carry an info
#: string (GFM: a backtick fence's info string may not itself contain a
#: backtick); the CLOSING fence is the delimiter alone.
_FENCE_DELIMITER: Final = re.compile(r"^ {0,3}(?:>\s*)*(`{3,}|~{3,})(.*)$")

#: A line-comment token inside a fenced block: SQL/Lua `--`, C++/JS `//`,
#: or shell/YAML/Python `#`. A line comment runs to end of line, so
#: adjacent comment lines inside a fence are SIBLING statements, never
#: folded (the same law the YAML comment units encode).
_FENCE_COMMENT_PREFIXES: Final = ("--", "//", "#")


def _fence_delim(raw: str, open_char: str | None, open_len: int) -> tuple[str, str, int] | None:
    """Classify one line against the fence state: `("open", char, length)`,
    `("close", char, length)`, or None (an ordinary line — inside an open
    fence every look-alike is content)."""
    match = _FENCE_DELIMITER.match(raw)
    if match is None:
        return None
    run, info = match.group(1), match.group(2)
    char, length = run[0], len(run)
    if open_char is not None:
        if char == open_char and length >= open_len and info.strip() == "":
            return ("close", char, length)
        return None  # any other delimiter look-alike is fence content
    if char == "`" and "`" in info:
        return None  # GFM: a backtick fence's info string may not contain a backtick
    return ("open", char, length)


def _markdown_units(path: str, text: str) -> Iterator[Unit]:
    """Markdown STRUCTURAL units (the semantic local units). The
    blank-line block splitter alone folded STRUCTURAL SIBLINGS into one
    unit, so a scoping marker in one sibling purified a universal gloss
    in another — the defect the round-4 NEW-4 attack demonstrated for
    table rows and the round-6 M-1 attack for HTML elements and YAML
    comment lines, and the round-7 N-1 attack demonstrated for the
    surface family that carries most of the docs: TIGHT LISTS, BLOCKQUOTE
    PARAGRAPHS, and FENCED BLOCKS. The units are now:

    * every TIGHT LIST ITEM is its own unit — a list marker (`-`, `*`,
      `+`, `N.`, `N)`) starts a unit at its own indentation, nested
      markers included (each nested item its own unit), and the item's
      own continuation lines stay with it (a wrapped item is one
      statement). A line the item cannot own (indented below the item's
      content column) is its own paragraph unit — a line a sibling could
      purify is never folded into it;
    * every BLOCKQUOTE PARAGRAPH is its own unit — a `>`-only separator
      line becomes the inner blank that starts the next unit, and `>>`
      nesting is stripped before the inner text is split by THIS SAME
      STRUCTURE, so a list or paragraph inside a quote splits the same
      way;
    * every FENCED BLOCK is its own unit — the delimiters and the
      fence's inner content never merge with the surrounding prose, two
      adjacent fences are separate units, and every LINE-COMMENT line
      inside a fence is ALSO its own unit (additive, like the HTML
      element units: an extra unit can only add a red, never hide one),
      so sibling comment statements can never fold. The fence's
      non-comment content stays the one statement it is;
    * table rows keep their round-4 per-row units, and a plain wrapped
      paragraph stays the one unit it is.

    The structural split REPLACES the blank-line block — a tight list is
    no longer ALSO one coarse unit, because the coarse unit was the
    laundering vector. Fence delimiters and table header/separator rows
    carry no detector vocabulary and the rules never fire on them, but
    their spans stay addressable for the allowlist's digest law. The
    outer pipes remain OPTIONAL (the round-5 F-2 attack): a row is any
    line carrying an unescaped pipe in a TABLE CONTEXT — a preceding
    header+separator pair, or the same contiguous run."""
    yield from _markdown_block_units(path, list(enumerate(text.splitlines(), start=1)))


def _markdown_block_units(path: str, lines: list[tuple[int, str]]) -> Iterator[Unit]:
    """One markdown nesting level: fenced blocks first (document-level,
    quote-marker-tolerant delimiters), then blockquote runs (recursed on
    their inner text), then the flat paragraph/list/table machine over
    the remaining runs."""
    segments: list[tuple[bool, list[tuple[int, str]]]] = []
    open_char: str | None = None
    open_len = 0
    for lineno, raw in lines:
        delim = _fence_delim(raw, open_char, open_len)
        if open_char is not None:
            segments[-1][1].append((lineno, raw))
            if delim is not None:
                open_char = None
            continue
        if delim is not None:
            open_char, open_len = delim[1], delim[2]
            segments.append((True, [(lineno, raw)]))
            continue
        if segments and not segments[-1][0]:
            segments[-1][1].append((lineno, raw))
        else:
            segments.append((False, [(lineno, raw)]))
    for is_fence, segment in segments:
        if is_fence:
            yield from _fence_units(path, segment)
        else:
            yield from _quote_or_prose_units(path, segment)


def _fence_units(path: str, segment: list[tuple[int, str]]) -> Iterator[Unit]:
    """The fenced block's own units: the WHOLE fence (delimiters and
    inner content — a single fenced block's content is the one statement
    it is, never merged with the surrounding prose, two adjacent fences
    separate), plus — additively — each line-comment line inside it as
    its own unit."""
    yield Unit(
        path,
        "markdown",
        segment[0][0],
        segment[-1][0],
        "\n".join(raw for _, raw in segment),
    )
    first = _FENCE_DELIMITER.match(segment[0][1])
    open_char = first.group(1)[0] if first else "`"
    open_len = len(first.group(1)) if first else 3
    closed = len(segment) > 1 and _fence_delim(segment[-1][1], open_char, open_len) is not None
    body = segment[1:-1] if closed else segment[1:]
    for lineno, raw in body:
        if raw.strip().startswith(_FENCE_COMMENT_PREFIXES):
            yield Unit(path, "markdown", lineno, lineno, raw)


def _quote_or_prose_units(path: str, segment: list[tuple[int, str]]) -> Iterator[Unit]:
    """Split a prose segment into blockquote runs and plain runs. A
    non-blank, non-marker line directly after a quote line is the GFM
    lazy continuation of the quote paragraph and stays in the run; every
    other non-quote line starts or joins the plain run."""
    runs: list[tuple[bool, list[tuple[int, str]]]] = []
    in_quote = False
    for lineno, raw in segment:
        if _QUOTE_PREFIX.match(raw) is not None:
            if in_quote:
                runs[-1][1].append((lineno, raw))
            else:
                runs.append((True, [(lineno, raw)]))
            in_quote = True
            continue
        if in_quote and raw.strip() and _LIST_MARKER.match(raw) is None:
            runs[-1][1].append((lineno, raw))  # the lazy continuation
            continue
        in_quote = False
        if runs and not runs[-1][0]:
            runs[-1][1].append((lineno, raw))
        else:
            runs.append((False, [(lineno, raw)]))
    for is_quote, run in runs:
        if is_quote:
            inner: list[tuple[int, str]] = []
            for lineno, raw in run:
                match = _QUOTE_PREFIX.match(raw)
                inner.append((lineno, raw[match.end() :] if match else raw))
            yield from _markdown_block_units(path, inner)
        else:
            yield from _flat_prose_units(path, run)


def _flat_prose_units(path: str, segment: list[tuple[int, str]]) -> Iterator[Unit]:
    """The flat machine: table rows stay per-row units (the round-4
    law), every tight list item is its own unit (nested markers
    included; continuation lines indented to the item's content column
    stay with the item), and the remaining blank-line blocks are the
    paragraphs they are. A plain line indented below the open item's
    content column ends the item and starts a paragraph — it is never
    folded into a sibling item's unit."""
    lines = [raw for _, raw in segment]
    units: list[Unit] = []
    para: list[tuple[int, str]] = []
    item: list[tuple[int, str]] = []
    item_indent = 0
    in_table = False  # the previous non-blank line was a table row/separator

    def emit(block: list[tuple[int, str]]) -> None:
        if block:
            units.append(
                Unit(
                    path,
                    "markdown",
                    block[0][0],
                    block[-1][0],
                    "\n".join(t for _, t in block),
                )
            )

    for pos, (idx, line) in enumerate(segment):
        stripped = line.strip()
        if not stripped:
            emit(para)
            para = []
            emit(item)
            item = []
            in_table = False
            continue
        following = lines[pos + 1].strip() if pos + 1 < len(lines) else ""
        is_row = _has_unescaped_pipe(stripped) and (in_table or _is_table_separator(following))
        if is_row:
            emit(para)
            para = []
            emit(item)
            item = []
            units.append(Unit(path, "markdown", idx, idx, stripped))
            in_table = True
            continue
        marker = _LIST_MARKER.match(line)
        if marker is not None:
            emit(para)
            para = []
            emit(item)
            item = [(idx, line)]
            item_indent = marker.end()
            in_table = False
            continue
        in_table = False
        if item and len(line) - len(line.lstrip()) >= item_indent:
            item.append((idx, line))
            continue
        emit(item)
        item = []
        para.append((idx, line))
    emit(para)
    emit(item)
    yield from units


def _yaml_scalar_units(path: str, text: str) -> Iterator[Unit]:
    """YAML scalar values (parsed; the folded text is one unit)."""
    try:
        import yaml
    except ImportError:  # pragma: no cover - the dependency ships with the test env
        return
    try:
        docs = list(yaml.safe_load_all(text))
    except Exception as exc:
        # Fail closed: an unreadable YAML is red, never skipped.
        raise AssertionError(f"{path}: unreadable YAML ({exc})") from exc
    lines = text.splitlines()

    def _walk(node: Any) -> Iterator[str]:
        if isinstance(node, dict):
            for value in node.values():
                yield from _walk(value)
        elif isinstance(node, list):
            for value in node:
                yield from _walk(value)
        elif isinstance(node, str):
            yield node

    for doc in docs:
        for scalar in _walk(doc):
            # The unit's span: the folded scalar's own text (the location
            # is the file's whole scalar; the digest is the folded text's).
            first_line = next(
                (i for i, line in enumerate(lines, 1) if scalar.splitlines()[0][:40] in line),
                1,
            )
            yield Unit(path, "yaml", first_line, first_line + len(scalar.splitlines()) - 1, scalar)


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
    for idx, line in enumerate(text.splitlines(), start=1):
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


def _html_units(path: str, text: str) -> Iterator[Unit]:
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
    statement it is. The blank-line markdown fallback is kept
    additively for the text the element grammar does not own (Jinja
    statements, a template's bare text) — the element units ADD
    granularity and can never remove a red."""
    line_starts = [0] + [match.end() for match in re.finditer("\n", text)]

    def line_of(offset: int) -> int:
        return bisect.bisect_right(line_starts, offset)

    parser = _HTMLTemplateParser(line_starts)
    try:
        parser.feed(text)
        parser.close()
    except AssertionError as exc:  # fail closed: an unparseable template is red
        raise AssertionError(f"{path}: unparseable HTML ({exc})") from exc

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
    for start, end in elements:
        cursor = start
        for child_start, child_end in children.get((start, end), ()):
            if child_start > cursor:
                yield Unit(
                    path,
                    "html-element",
                    line_of(cursor),
                    line_of(child_start),
                    text[cursor:child_start],
                )
            cursor = max(cursor, child_end)
        if cursor < end:
            yield Unit(path, "html-element", line_of(cursor), line_of(end), text[cursor:end])

    # The comment statements (exact raw spans).
    for kind, offset, _data in parser.tokens:
        if kind != "comment" or not text.startswith("<!--", offset):
            continue
        close = text.find("-->", offset + 4)
        end = len(text) if close == -1 else close + 3
        yield Unit(path, "html-comment", line_of(offset), line_of(end), text[offset:end])
    for match in re.finditer(r"\{#.*?#\}", text, re.DOTALL):
        yield Unit(
            path,
            "template-comment",
            line_of(match.start()),
            line_of(match.end()),
            match.group(),
        )


def _python_units(path: str, text: str, tree: ast.Module) -> Iterator[Unit]:
    """AST docstrings, contiguous comment blocks, `Field(description=...)`
    strings, and the typed collection literals (rule 4's own units)."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                doc_node = (
                    node.body[0]
                    if getattr(node, "body", None) and isinstance(node.body[0], ast.Expr)
                    else None
                )
                first = doc_node.lineno if doc_node is not None else getattr(node, "lineno", 1)
                yield Unit(path, "docstring", first, first + len(doc.splitlines()) - 1, doc)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            # Every string constant is a unit (Field(description=...) strings
            # included — the constant arm catches them; no parent chain is
            # needed and the AST is walked without parent linking).
            if node.lineno == node.end_lineno:
                yield Unit(path, "string", node.lineno, node.lineno, node.value)
            else:
                yield Unit(path, "string", node.lineno, node.end_lineno or node.lineno, node.value)
        # Rule 4's structural enumerations: Literal[...] subscripts and
        # tuple/list/set literals whose elements are the reason labels.
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id == "Literal"
        ):
            seg = ast.get_source_segment(text, node) or ""
            yield Unit(path, "literal-type", node.lineno, node.end_lineno or node.lineno, seg)
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            seg = ast.get_source_segment(text, node) or ""
            yield Unit(path, "collection", node.lineno, node.end_lineno or node.lineno, seg)

    # Contiguous comment blocks (the AST carries comments only via tokenize).
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


def rule_closed_enumeration(unit: Unit) -> bool:
    """Rule 1: the unit DEFINES the stranded reason family but supplies
    only the old dispatch conditions — the two-label closed set, the
    prose two-member form, OR the single-member definition (the
    settings-description class: defining the entire check by no-config
    alone is the same defect; the rule never requires the word 'two').
    A corrected unit either includes `workflow_root_missing` and its
    distinct meaning, or expressly says it is limited to the
    dispatch-invisible subset."""
    if not _unit_mentions_detector(unit):
        return False
    lowered = _lowered(unit)
    if "workflow_root_missing" in lowered:
        return False
    if _expressly_scoped(unit):
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
    # THE DEFINING FRAMING: the unit presents itself as the family's own
    # definition (the whole set, the check's meaning, the gauge's
    # population) — one old condition plus this framing is the closed
    # enumeration.
    defining = any(
        phrase in lowered
        for phrase in (
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
            "the stranded-jobs detector",
            "the stranded detector",
            "stranded-jobs detector",
            "that is the whole",
            "can never " + "dispatch",
            "can never be " + "dispatched",
            "that cannot make progress",
            "jobs that can never",
            "rows that can never",
            "stranded jobs",
            "stranded rows",
        )
    )
    return defining


def _expressly_scoped(unit: Unit) -> bool:
    lowered = _lowered(unit)
    return any(marker in lowered for marker in _SCOPED_MARKERS)


def rule_universal_no_dispatch(unit: Unit) -> bool:
    """Rule 2: a universal no-dispatch/no-touch/invisible claim attached
    to the stranded detector/gauge that the same local unit does not
    expressly scope to the dispatch-invisible legs."""
    if not _unit_mentions_detector(unit):
        return False
    lowered = _lowered(unit)
    if not any(variant in lowered for variant in _UNIVERSAL_VARIANTS):
        return False
    return not _expressly_scoped(unit)


def rule_whole_detector_parity(unit: Unit) -> bool:
    """Rule 3: a whole-detector parity/mirror claim that the unit does not
    expressly narrow to the two dispatch-invisible legs."""
    if not _unit_mentions_detector(unit):
        return False
    lowered = _lowered(unit)
    if not any(variant in lowered for variant in _PARITY_VARIANTS):
        return False
    return not _expressly_scoped(unit)


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
    return not _expressly_scoped(unit)


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
    SAME units the scan matches). Fail-closed: a .py file that does not
    parse raises, unreadable YAML raises, an unparseable template
    raises — never a silent skip."""
    if rel.endswith(".py"):
        tree = ast.parse(raw)
        return list(_python_units(rel, raw, tree))
    if rel.endswith((".yml", ".yaml")):
        return (
            list(_yaml_scalar_units(rel, raw))
            + list(_yaml_comment_units(rel, raw))
            + list(_markdown_units(rel, raw))
        )
    if rel.endswith(".html"):
        return list(_html_units(rel, raw)) + list(_markdown_units(rel, raw))
    return list(_markdown_units(rel, raw))


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
    * a wildcard entry's file still CARRIES the class's vocabulary (an
      emptied exception is a stale one).

    Returns one human-readable violation per defect; empty = every
    entry is live."""
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
        if not any(unit.span_digest == digest for unit in units):
            violations.append(
                f"{allowed_path}: the allowlisted span's digest matches NO live unit "
                f"({digest}) — the span was edited or deleted; re-derive or prune "
                "the entry"
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
    block's content stay the ONE unit they are (a statement is never
    fragmented into meaningless pieces). Every gloss value is assembled
    at runtime from independently non-triggering fragments (the module's
    own law)."""
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
    assert len(plain_units) == 1, [unit.text for unit in plain_units]
    two_fences = "```sql\nSELECT 1;\n```\n```sql\nSELECT 2;\n```"
    fence_units = _corpus_units("virtual/fence-two.md", two_fences)
    assert len(fence_units) == 2, [unit.text for unit in fence_units]
    prose_then_fence = (
        "The dispatch-invisible legs are scoped.\n" + "```sql\n" + gloss_comment + "\n```"
    )
    prose_fence_units = _corpus_units("virtual/fence-prose-gloss.md", prose_then_fence)
    firing_units = [unit for unit in prose_fence_units if any(rule(unit) for _n, rule in RULES)]
    assert any(unit.text.strip() == gloss_comment for unit in firing_units), (
        "a fence never merges with its surrounding prose — the prose's marker "
        f"cannot purify the fence's gloss comment's own unit: {[u.text for u in firing_units]}"
    )
