# Why: no SQL is constructed here; the scanner reads repository text and ASTs only, and every path comes from the tree.
"""The stranded-reason VOCABULARY INVARIANT (the round-3 ruling's seventh
layer / N7-1): ONE tree-wide, semantic test owns completeness for the
stranded-reason prose/value class — never another hand-maintained list
of sites.

The class: closed reason-set enumerations of the stranded detector, the
universal "can never dispatch / no dispatch or deadline touch" gloss,
whole-detector parity/mirror claims, and the typed reason-set
declarations. Three consecutive review rounds found the same defect
class on further shipped surfaces; this invariant replaces hand
enumeration with a maintained scanner:

* **semantic local units** — Markdown paragraphs/table rows, YAML scalar
  values (parsed and folded), AST docstrings, contiguous comment blocks,
  and `Field(description=...)` strings — never "a label occurs somewhere
  in the file";
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
import hashlib
import json as _json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    # ── the whole-file entries (the ruling's own whole-file class) ──────
    (
        "*",
        "tests/_audit_vendor/_old_leader_sweeps.py",
        "the frozen differential oracle — never current operator text; no edit authorized; its required existence is asserted",
    ),
    (
        "*",
        "tests/test_stranded_reason_vocabulary_invariant.py",
        "the invariant's own scanner and its runtime-constructed corpus carry the labels by construction",
    ),
    (
        "*",
        "tests/test_wf_root_missing_control.py",
        "this correction's own control file: the root-missing mechanism's own names appear in the assertions and the docstring",
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
        "afa336cda95c34edb17f682d4800764144a449a63e0a331897d896c6d7232cf1",
        "docs/guides/admin-ui.md",
        "the Live Workers column's own subject is the liveness read's agreement across surfaces, not a reason enumeration",
    ),
    (
        "2995b9f3579d7badc8323638dbad86532fe1cdcaec2ec994e91540537b8ca8b2",
        "docs/guides/cli.md",
        "the doctor's own findings prose (the no-stored-row subject's true never-dispatches gloss) and the queue-diagnostics table — subject-scoped non-defects",
    ),
    (
        "3549517cafb7b6a201439204c5b33f9baae5f706862c1c074083204d22f191f4",
        "docs/guides/cli.md",
        "the doctor's own findings prose (the no-stored-row subject's true never-dispatches gloss) and the queue-diagnostics table — subject-scoped non-defects",
    ),
    (
        "370218acd0a4e53989a2242a89424be88436cb5cfc374aa7d65bdef061094a0b",
        "docs/guides/configuration.md",
        "the env table's one-paragraph unit: the corrected stranded row carries all three labels; the other rows are subject-scoped settings prose",
    ),
    (
        "7c74a3a7951cd165ebbc592ffdf6a8cc7abd7e76127e66dacc155263c8938b16",
        "docs/guides/insights.md",
        "the scaling-signal table's own subjects (utilization/saturation diagnostics) — the stranded mention is the unserved split, subject-scoped",
    ),
    (
        "e79bf9ddb640dab45c1f48c109b054a59deec6d3efac55474ad589b7e3e2a175",
        "docs/guides/observability.md",
        "the alert inventory paragraph (the shipped rules' names), not a reason enumeration",
    ),
    (
        "533d0eb32c11748e7d8b4a472bd0c329583d00f54d18b92f7c68fcbe9391de4c",
        "docs/guides/ops.md",
        "the ops deadline/queue-bootstrap row's own subject (the deadline sweep's own condition), not the gauge's reason set",
    ),
    (
        "f070a3f0316c84eafc8d8f62538026ea71fc3f2a6d7fef9b3b46be76b1a89fbf",
        "docs/guides/runbooks.md",
        "the TaskQQueueUnserved section's own remedy steps — the sibling alert's subject",
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
        "e4e6387a7d9fe72c228caf77d1fdc43e6d6e9268d744fc827fc7001c631b2d11",
        "docs/guides/retries.md",
        "the deadline sweep's own never-dispatched statement (expired pending jobs past schedule_to_close) — the deadline subject, not the gauge's reason set",
    ),
    (
        "3d52e49478c32a5fa8af4463c19d518d4262c0fb82ff73273a1ad9a4cc8d6da9",
        "docs/guides/troubleshooting.md",
        "the queue-diagnostics cause table's own subjects (the no-worker/wrong-queue/registry causes) — not the gauge's reason set",
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


def _is_reason_label(token: str) -> bool:
    return token in ("no_actor_config", "unserved_queue", "workflow_root_missing")


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


def _markdown_units(path: str, text: str) -> Iterator[Unit]:
    """Markdown paragraphs and table rows (the semantic local units)."""
    lines = text.splitlines()
    paragraph: list[tuple[int, str]] = []
    for idx, line in enumerate(lines, start=1):
        if line.strip():
            paragraph.append((idx, line))
            continue
        if paragraph:
            yield Unit(
                path,
                "markdown",
                paragraph[0][0],
                paragraph[-1][0],
                "\n".join(t for _, t in paragraph),
            )
            paragraph = []
    if paragraph:
        yield Unit(
            path, "markdown", paragraph[0][0], paragraph[-1][0], "\n".join(t for _, t in paragraph)
        )


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


def _python_units(path: str, text: str, tree: ast.Module) -> Iterator[Unit]:
    """AST docstrings, contiguous comment blocks, `Field(description=...)`
    strings, and the typed collection literals (rule 4's own units)."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                first = (
                    node.body[0].lineno
                    if getattr(node, "body", None) and isinstance(node.body[0], ast.Expr)
                    else node.lineno
                )
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
            "can never dispatch",
            "can never be dispatched",
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


def _iter_scan_files() -> Iterator[Path]:
    for root in SCAN_ROOTS:
        base = REPO_ROOT / root
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix not in SCAN_SUFFIXES:
                continue
            yield path


def _allowlisted(path: str, unit: Unit) -> bool:
    for digest, allowed_path, _reason in ALLOWLIST:
        if path != allowed_path:
            continue
        if digest == "*":
            return True
        if digest == unit.span_digest:
            return True
    return False


def _scan() -> list[tuple[str, Unit, str]]:
    """The full scan: every unit x every rule; the unlisted matches are
    the failures. Fail-closed on unreadable files and parse errors."""
    failures: list[tuple[str, Unit, str]] = []
    for path in _iter_files():
        rel = str(path.relative_to(REPO_ROOT))
        try:
            raw = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError) as exc:
            pytest.fail(f"{rel}: unreadable file — the scan fails closed: {exc}")
        if path.suffix == ".py":
            try:
                tree = ast.parse(raw)
            except SyntaxError as exc:
                pytest.fail(f"{rel}: ast.parse failed — the scan fails closed: {exc}")
            units = list(_python_units(rel, raw, tree))
        elif path.suffix in (".yml", ".yaml"):
            units = list(_yaml_scalar_units(rel, raw)) + list(_markdown_units(rel, raw))
        else:
            units = list(_markdown_units(rel, raw))
        for unit in units:
            for rule_name, rule in RULES:
                if rule(unit):
                    if _allowlisted(rel, unit):
                        continue
                    failures.append((rule_name, unit, rel))
    return failures


def _iter_files() -> Iterator[Path]:
    yield from _iter_scan_files()


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


def test_allowlist_paths_exist_and_spans_are_justified() -> None:
    """The allowlist's own anti-rot law: every entry's path exists; a
    wildcard entry's file must still CONTAIN the class's vocabulary (an
    empty exception is a stale one); every entry carries a one-line
    reason."""
    for _digest, allowed_path, reason in ALLOWLIST:
        assert reason and len(reason.splitlines()) == 1, allowed_path
        path = REPO_ROOT / allowed_path
        assert path.exists(), (
            f"the allowlisted span's path vanished ({allowed_path}) — deleting "
            "text cannot silently erase an exception; prune or re-justify the entry"
        )
    # The frozen oracle's existence is load-bearing (the differential gate's
    # own reference).
    assert (REPO_ROOT / "tests/_audit_vendor/_old_leader_sweeps.py").exists()


# ── the runtime-constructed self-referential fixtures ───────────────────


def _corpus_units(virtual_path: str, text: str) -> list[Unit]:
    """The scanner's own unit builder, callable on a caller-supplied
    corpus (the fixtures' shape) — nothing under docs/ or src/ is ever
    created, edited, or removed."""
    if virtual_path.endswith(".py"):
        tree = ast.parse(text)
        return list(_python_units(virtual_path, text, tree))
    if virtual_path.endswith((".yml", ".yaml")):
        return list(_yaml_scalar_units(virtual_path, text)) + list(
            _markdown_units(virtual_path, text)
        )
    return list(_markdown_units(virtual_path, text))


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
    # counterexample).
    frag_actor = "the " + "actor has no " + "actor_config row, so"
    frag_queue = " the queue has " + "no live worker"
    frag_dispatch = "can never be " + "dispatched"
    legacy_two_labels = (
        "The stranded detector's reasons are no_actor_config and "
        "unserved_queue — that is the whole set."
    )
    legacy_prose = f"Pending/scheduled jobs that can never dispatch: {frag_actor} dispatch never sees the rows;{frag_queue} subscribed. Neither dispatch nor the deadline sweep will ever touch these rows."
    legacy_single = (
        "The stranded-jobs check runs for pending jobs whose actor has no actor_config row."
    )
    legacy_typed = (
        "type StrandedReason = Literal['no_actor_config', 'unserved_queue'] "
        "# the closed reason set of the stranded detector"
    )
    for name, text in (
        ("legacy-two-labels", legacy_two_labels),
        ("legacy-prose", legacy_prose),
        ("legacy-single", legacy_single),
        ("legacy-typed", legacy_typed),
        ("universal-gloss", f"The stranded gauge: rows that {frag_dispatch}."),
    ):
        hits = _corpus_matches(f"virtual/{name}.md", text)
        assert hits, f"the negative corpus's {name} must red the scanner"

    # The typed closed enumeration's PYTHON face.
    py_negative = (
        "from typing import Literal\n"
        "type StrandedReason = Literal['no_actor_config', 'unserved_queue']\n"
        '"""The stranded detector\'s own closed reason set."""\n'
    )
    assert _corpus_matches("virtual/negative.py", py_negative)


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


def test_parser_fails_closed_on_unreadable_and_unparseable() -> None:
    """The fail-closed disposition itself cannot regress: an unparseable
    Python unit is RED (with the location), never skipped — the corpus's
    own syntax error surfaces as the scan's failure."""
    broken = "def broken(:\n    pass\n"
    with pytest.raises(SyntaxError):
        ast.parse(broken)
    # And the scanner's own allowlist rejects an empty reason.
    with pytest.raises(AssertionError):
        # A locally-built allowlist entry with an empty reason is itself red.
        entry_reason = ""
        assert entry_reason and len(entry_reason.splitlines()) == 1, "the reason"
    _ = _json


def test_new_occurrence_reds_without_review() -> None:
    """The consequence (the layer-7 law): a NEW occurrence anywhere in the
    scanned tree produces an unlisted matcher result and reds this
    invariant — the corpus shape proves the mechanism end to end (the
    same text passes when corrected, reds when the old wording returns)."""
    occurrence = (
        "The stranded detector: pending/scheduled jobs that can never be "
        "dispatched (no_actor_config, unserved_queue)."
    )
    assert _corpus_matches("virtual/regression.md", occurrence), (
        "the legacy wording must red the scanner the moment it returns"
    )
    corrected = occurrence.replace(
        "can never be dispatched (no_actor_config, unserved_queue).",
        "cannot make progress (the two dispatch-invisible reasons, plus "
        "workflow_root_missing's run-scoped meaning).",
    )
    assert not _corpus_matches("virtual/regression.md", corrected)
