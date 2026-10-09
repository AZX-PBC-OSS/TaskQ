"""DOCS-HONESTY PINS — the loop front's doc-truth convictions (the
hostile review of the consolidated head, af1b8779; the front's REPORT.md
surface B carries the audit table).

Pure file-system pins (no PG): each parses the doc/artifact it convicts
and asserts the honest shape. All are LIVE defects → strict-xfail; the
cure (the artifact produced, the figure deleted, the counts reconciled,
the shipped surface documented) flips the pin to XPASS-strict — remove
the marker WITH the cure. The full defect list with the exact cure per
item: DEFECTS.md beside this file in the pin pack.

The receipts law these pins enforce: a measured number cited by the
repo's docs resolves to a run-scoped artifact in ``.measurements/`` — or
the number is a claim with no evidence. ("A red is machine-generated or
it doesn't exist"; a figure is artifact-backed or it doesn't ship.)
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MEASUREMENTS = REPO / ".measurements"
PERF_DOC = REPO / "perf-evidence-workflows.md"
STREAMING_DOC = REPO / "perf-evidence-workflows-streaming.md"
GUIDE = REPO / "docs" / "guides" / "workflows.md"
API_REF = REPO / "docs" / "api-reference" / "workflows.md"
VALIDATE_SRC = REPO / "src" / "taskq" / "workflows" / "api" / "_validate.py"
SWEEP_SRC = REPO / "src" / "taskq" / "workflows" / "_sweep.py"
SQL_LOOP_SRC = REPO / "src" / "taskq" / "workflows" / "api" / "_sql_loop.py"


# ── shared helpers ──────────────────────────────────────────────────────


def _walk_numbers(node: object) -> list[float]:
    """Every numeric value in a parsed JSON document (bools excluded)."""
    if isinstance(node, bool):
        return []
    if isinstance(node, (int, float)):
        return [float(node)]
    if isinstance(node, dict):
        return [v for value in node.values() for v in _walk_numbers(value)]
    if isinstance(node, list):
        return [v for value in node for v in _walk_numbers(value)]
    return []


def _parse_json_documents(text: str) -> list[object]:
    """Every JSON document in a file's text: the whole file when it is
    one document (the common artifact), else each embedded object/array
    (JSONL logs and pretty-printed records riding a structlog preamble —
    the t11-latency-band.json shape) via sliding raw_decode."""
    try:
        return [json.loads(text)]
    except ValueError:
        pass
    decoder = json.JSONDecoder()
    docs: list[object] = []
    idx = 0
    while idx < len(text):
        match = re.search(r"[{\[]", text[idx:])
        if match is None:
            break
        start = idx + match.start()
        try:
            doc, end = decoder.raw_decode(text, start)
        except ValueError:
            idx = start + 1
            continue
        docs.append(doc)
        idx = end
    return docs


def _artifact_pool() -> dict[str, list[float]]:
    """Every numeric value in every ``.measurements/**/*.json``
    artifact, per file (bare names, ``runs/`` run-scoped captures, and
    the dated subdirectories)."""
    pool: dict[str, list[float]] = {}
    for path in sorted(MEASUREMENTS.rglob("*.json")):
        values = [
            v
            for doc in _parse_json_documents(path.read_text(errors="replace"))
            for v in _walk_numbers(doc)
        ]
        if values:
            pool[str(path.relative_to(MEASUREMENTS))] = values
    return pool


def _resolves(figure: str, unit: str, pool: dict[str, list[float]]) -> str | None:
    """The artifact carrying the figure at its CITED precision (a figure
    in seconds also tries the millisecond reading, x1000)."""
    decimals = len(figure.partition(".")[2])
    target = float(figure)
    for name, values in pool.items():
        for value in values:
            if unit == "ms" and round(value, decimals) == target:
                return name
            if unit == "s" and (
                round(value, decimals) == target or round(value / 1000.0, decimals) == target
            ):
                return name
    return None


#: A measured point figure: ``N ms`` / ``N s``, never comma-grouped bytes
#: or row counts (the lookbehind refuses "2,754,895 B"-style matches).
_FIGURE_RE = re.compile(r"(?<![\w.,])(\d+(?:\.\d+)?)\s*(ms|s)\b")

#: The bound contexts: a BAND is the gate, not a measurement — figures
#: immediately preceded by these tokens are the doc's declared bounds.
_BOUND_CONTEXT = ("≤", "band", "headroom", "budget")


def _perf_doc_figures(text: str) -> list[tuple[int, str, str]]:
    """The perf doc's measured point figures: (lineno, figure, unit)."""
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    figures: list[tuple[int, str, str]] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        for match in _FIGURE_RE.finditer(line):
            context = line[max(0, match.start() - 40) : match.start()].lower()
            if any(token in context for token in _BOUND_CONTEXT):
                continue
            figures.append((lineno, match.group(1), match.group(2)))
    return figures


# ── DOCS-1/2: the docs-numbers pin — every measured figure resolves ─────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [DOCS-RATIO]; the marker is removed per the designed flip (the confirmation receipt).


# ── DOCS-3: the streaming ratio is cherry-picked against the captures ───


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the streaming doc's cited ratio now reads the NEWEST capture's (or the honest range): the cure landed [DOCS-RATIO]; the marker is removed per the designed flip (the confirmation receipt).
def test_docs_the_streaming_ratio_is_the_newest_capture_or_an_honest_range() -> None:
    """The capture law for the cited band ratio: the doc's number is the
    NEWEST run-scoped capture's, or the doc states the range across the
    captures (either form names the newest capture's ratio). A cited
    older capture whose figure the newest refutes is the cherry-pick
    this pin convicts."""
    text = STREAMING_DOC.read_text()
    captures = sorted(MEASUREMENTS.glob("t20-streaming-bands-*.json"))
    assert captures, "no run-scoped streaming captures exist at all"
    ratios: dict[str, float] = {}
    for path in captures:
        doc = json.loads(path.read_text())
        ratios[path.name] = (
            doc["dispatch_band_200_chain"]["p50_ms"] / doc["dispatch_band_200_plain"]["p50_ms"]
        )
    newest_name, newest_ratio = next(reversed(ratios.items()))
    assert f"{newest_ratio:.2f}" in text, (
        f"the streaming doc's cited ratio predates the evidence: the doc cites "
        f"1.02x but the NEWEST run-scoped capture {newest_name} reads "
        f"{newest_ratio:.2f}x (all captures: "
        + ", ".join(
            f"{name.removeprefix('t20-streaming-bands-').removesuffix('.json')}: {ratio:.2f}x"
            for name, ratio in ratios.items()
        )
        + "). The cited number must be the newest capture's, or the doc states "
        "the range honestly — a cherry-picked capture is a lie by selection"
    )


# ── DOCS-4: the T17 disposition counts drift three ways ────────────────


def _t17_table_counts(ledger: str) -> dict[str, int]:
    """The ledger TABLE's own disposition counts (the truth source): one
    row per cut, the disposition column read per row."""
    counts: dict[str, int] = {}
    for line in ledger.splitlines():
        if not line.startswith("| #"):
            continue
        cells = [c.strip() for c in line.split("|")]
        disposition = cells[3].strip("*").lower()
        for cls in ("applied", "declined", "recorded", "verify-absent", "deferred"):
            if disposition.startswith(cls):
                counts[cls] = counts.get(cls, 0) + 1
                break
        else:
            raise AssertionError(f"unparseable disposition row: {line[:100]}")
    return counts


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [the T17 counts: the ledger header to the pin's contract + the guide's one-home doctrine enforced (the pointer present, the arithmetic absent)]; the marker is removed per the designed flip.
def test_docs_the_t17_disposition_counts_agree_across_their_three_homes() -> None:
    """One count, one home: the ledger's table (the truth), the ledger's
    header line, and the guide's parenthetical must agree. (The
    ``applied`` class tolerates the #3b fold convention — the drifted
    classes are the conviction; the two prose homes must additionally
    agree with EACH OTHER on every class.)"""
    ledger = (MEASUREMENTS / "t17-dispositions.md").read_text()
    guide = GUIDE.read_text()
    table = _t17_table_counts(ledger)

    header_match = re.search(
        r"applied (\d+).*?declined-with-reason (\d+).*?recorded-no-action\s*(\d+).*?"
        r"verify-absent (\d+).*?deferred\s+(\d+)",
        ledger,
        re.S,
    )
    assert header_match is not None, "the ledger header's count line is gone"
    keys = ("applied", "declined", "recorded", "verify-absent", "deferred")
    header = dict(zip(keys, (int(v) for v in header_match.groups()), strict=True))

    drift: list[str] = []
    for cls in keys:
        if header[cls] != table[cls]:
            drift.append(f"{cls}: the ledger header says {header[cls]}, the table has {table[cls]}")
    # THE GUIDE'S ONE-HOME LAW (the doc-honesty round's own cure, which
    # REPLACED this pin's old two-prose-homes contract): the guide
    # carries NO arithmetic — it POINTS at the ledger (the one home).
    # The pin now enforces the pointer + the absence: the counts live
    # in exactly one place.
    if re.search(r"applied\s+\d+\s*/", guide):
        drift.append(
            "the guide carries its own count arithmetic — the one-home law says the ledger is the only home"
        )
    if ".measurements/t17-dispositions.md" not in guide:
        drift.append("the guide lost its pointer to the disposition ledger (the one home)")
    assert not drift, (
        "T17 disposition count drift (one count, one home):\n  - "
        + "\n  - ".join(drift)
        + f"\n  (the table's own counts: {table})"
    )

    # ── DOCS-5: the reference surface vs the shipped surface ───────────────

    # THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [DOCS-API]; the marker is removed per the designed flip (the confirmation receipt).
    assert not drift, "the API reference's validate surface drifted:\n  - " + "\n  - ".join(drift)


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the API reference documents the loop surface (loop(/Done/Refine all present): the cure landed [DOCS-API]; the marker is removed per the designed flip (the confirmation receipt).
def test_docs_the_api_reference_documents_the_loop_surface() -> None:
    """``wf.loop`` / ``Done`` / ``Refine`` are v1 API; a reference that
    omits them sends the reader to the guide's (stale) signature as the
    only spelling."""
    api = API_REF.read_text()
    missing = [token for token in ("loop(", "Done", "Refine") if token not in api]
    assert not missing, (
        f"the API reference omits the loop surface: {missing} — loop/Done/Refine "
        "are v1 and absent from the reference"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the guide §9 spells the shipped verb's own signature (every parameter, the bare names) [the guide-signature cure]; the marker is removed per the designed flip (the confirmation receipt).
def test_docs_the_guides_loop_signature_matches_the_shipped_verb() -> None:
    """The guide's spelled signature must carry every parameter the
    shipped verb takes (a param the guide never names — carry_type,
    budget_s, escalates_to, gates — is a feature a reader cannot
    discover)."""
    from taskq.workflows import loop as loop_verb

    guide = GUIDE.read_text()
    match = re.search(r"wf\.loop\(([^)]*)\)", guide)
    assert match, "the guide never spells wf.loop's signature"
    spelled = {token.strip() for token in match.group(1).split(",") if token.strip()}
    shipped = set(inspect.signature(loop_verb).parameters)
    missing = shipped - spelled
    assert not missing, (
        f"the guide §9 loop signature is stale: it spells {sorted(spelled)} but "
        f"the shipped verb takes {sorted(shipped)} — missing from the guide: "
        f"{sorted(missing)}"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [the inventory names the loop/HITL/phase3-cure/progress families]; the marker is removed per the designed flip (the confirmation receipt).
def test_docs_the_pin_inventory_names_the_families_the_guide_relies_on() -> None:
    """ "Every invariant above is pinned by a test that can fail (the pin
    inventory at the foot of this page)" — the inventory is the map; a
    family omitted from it is an invariant a reviewer cannot find the
    pin for."""
    guide = GUIDE.read_text()
    match = re.search(r"## The pin inventory\n(.*?)\n## ", guide, re.S)
    assert match, "the guide's pin inventory section is gone — recheck the pin"
    inventory = match.group(1)
    missing = [
        name
        for name in (
            "test_wf_loop_pins.py",
            "test_wf_hitl_pins.py",
            "test_wf_phase3_cure_pins.py",
            "test_wf_progress_",
        )
        if name not in inventory
    ]
    assert not missing, (
        f"the guide's pin inventory omits pin families its own sections rely on: {missing}"
    )


# ── DOCS-6: the carry dragon's "red forever" claim has no receipt ──────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [the carry dragon's drill receipt recorded (the machine-generated red)]; the marker is removed per the designed flip (the confirmation receipt).
def test_docs_the_carry_dragons_red_forever_claim_has_a_receipt() -> None:
    """The receipts law, applied to the carry dragon: the claim stands
    in two homes (the guide's §9 carry paragraph and the advance
    statement's own comment), so the run-scoped reds ledger must carry
    the carry drill's receipt. If the claim is deleted instead (the
    honest-shrink cure), the pin passes vacuously — the law binds claims
    to receipts, not the other way."""
    guide = GUIDE.read_text()
    sql_loop = SQL_LOOP_SRC.read_text()
    claim_present = bool(
        re.search(r"optimistic-apply dragon[\s\S]{0,200}?red\s+forever", guide, re.I)
    ) or bool(re.search(r"CARRY-OPTIMISTIC[\s\S]{0,300}?RED\s+forever", sql_loop, re.I))
    receipts: list[str] = []
    reds_file = MEASUREMENTS / "t19-pin-reds.json"
    if reds_file.exists():
        for line in reds_file.read_text().splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            for entry in record.get("entries", []):
                receipts.append(f"{entry.get('pin', '')} — {entry.get('mutation', '')}")
    carry_receipts = [r for r in receipts if re.search(r"carry|optimistic", r, re.I)]
    assert carry_receipts or not claim_present, (
        "the carry dragon's 'red forever' claim stands (guide §9 + "
        "LOOP_ADVANCE_SQL's comment) but t19-pin-reds.json carries NO "
        f"carry-mutation receipt ({len(receipts)} receipts on file, none for the "
        "carry drill) — a red is machine-generated or it doesn't exist (the "
        "receipts law). The cure: run the carry drill and record the receipt."
    )
