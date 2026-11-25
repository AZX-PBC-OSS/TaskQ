"""The failure log law guard: a failure log entry CITES ITS RECEIPT
CHAIN, or it doesn't exist.

A failure log entry must cite (a) its scenario's RUN-ID, (b)
the scenario's captured output FILE PATH, (c) the capture's sha256 —
and the guard RE-VERIFIES at guard time: the cited file EXISTS, its
digest matches, and its content IS the recorded red. An entry whose
cited capture is missing or mismatching FAILS the guard — whatever
syntactic shape produced it.

An earlier guard body AST-walked every test module and
rejected ``.red(...)`` calls whose ``observed`` payload was composed
entirely of syntactic constants — a hand-written literal is prose
wearing an evidence file's clothes. That shape rule was defeatable
twice over:

1. **the keyword-form bypass** — ``log.red('pin', 'mut', observed={...})``
   has two positional args; the AST scan read ``node.args[2]``, found
   nothing, and SKIPPED INSPECTION. A fabricated payload riding the
   keyword never saw the guard.
2. **the name-laundered constant** — ``log.red('pin', 'mut',
   FABRICATED)`` where ``FABRICATED`` is a module-level literal dict: the
   payload node is an ``ast.Name``, not a literal — the scan passed it.
   The lie moved one hop up the module and the guard went blind.

The provenance rule replaces the shape rule: the guard inspects only
the SINK's receipt chain, never the call. The two bypasses are re-tested below and both
are rejected BY PROVENANCE: the keyword form and the laundered name
produce receipts like any other call, and a FABRICATED entry's verdict
is its broken receipt — a missing capture, a digest mismatch, a content
mismatch — never the payload's grammar.

(A mechanical guard cannot read minds: a pin that writes the same lie
into its scenario capture and its entry leaves a self-consistent artifact —
which is why the mutation tests' teeth are BEHAVIORAL pins
(``test_wf_sweep_pins.py``'s law): the lie flips a green test the
moment the mutation ships. The receipt chain covers the mechanical
half: hand-edited sinks, fabricated citations, stale
captures, cross-tree identity confusion.)
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
import warnings
from hashlib import sha256
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from tests._wf_fixtures import (
    _REPO_ROOT,
    MEASUREMENTS,
    REDLOG_CAPTURES_DIR_ENV,
    REDLOG_LAW,
    RedLog,
    head_sha,
    redlog_captures_root,
    resolve_redlog_capture,
    source_changes_since,
)

#: The checkout the guard runs from (resolved — the symlinked-checkout
#: anchor pin's reference point).
_REPO = Path(__file__).resolve().parents[1]

#: The run id the forgery pins build around: the record's and the
#: honest capture's own run field.
_FORGED_RUN = "20260101T000000-1-cafecafe"

#: The failure log sinks (the fixtures' filenames — every RedLog instance).
_SINKS: tuple[str, ...] = (
    "pin-reds.json",
    "ledger-pin-reds.json",
    "t10-pin-reds.json",
    "t19-pin-reds.json",
    "t06-propagation-reds.json",
    "t21-pin-reds.json",
)


def _head() -> str:
    """The current head's sha (the live record's scoping anchor)."""
    return head_sha()


#: The unpinned re-stamps already on ``origin/evidence/taskqflow`` whose cited
#: captures were written to an ephemeral temp root and never committed to the
#: estate: no tree can clear their receipts, and the evidence branch is not this
#: change's to rewrite. The staged fallback skips exactly these heads — never any
#: other — and each entry states WHY it is grandfathered, because the mapping is
#: a sticky exception list and the stale pin below
#: (``test_no_grandfathered_unpinned_restamp_outlives_its_reason``) demands the
#: removal of every entry whose reason has lapsed. Any NEW unpinned re-stamp is
#: outside the mapping, is appended last in its sink, and fails the guard loudly.
_GRANDFATHERED_UNPINNED_RESTSAMPS: dict[str, str] = {
    "4441dd642fbd47501d2830eba3622869551d68d5": (
        "Why: its 2026-11-09 battery ran with TASKQ_REDLG_CAPTURES_DIR unset, so "
        "the 175 receipts it cites sit under the ephemeral "
        "/tmp/taskq-redlog-captures-1000 root and were never committed to the "
        "estate; the later re-stamps repeated the same omission."
    ),
    "7f2bd32caa969643b3568911fdd79ffe74a25cbd": (
        "Why: the 2026-11-14 re-stamp repeated the unpinned battery — its 175 "
        "receipts cite the same ephemeral root, and the one batch newer than it "
        "(ffcb61fb37) carries the identical defect."
    ),
    "ffcb61fb37d63e2bb1f96314e8d76fa097b1a7f8": (
        "Why: the 2026-11-23 re-stamp is the newest batch on the branch and is "
        "still unpinned, so its 175 receipts cite the ephemeral root; a battery "
        "re-run with the captures pin set and pushed retires this entry."
    ),
}


class _SinkScan(NamedTuple):
    """One read of the sinks: the law-stamped records, the LIVENESS ANCHOR
    their head stamps are judged against, and which estate they came from
    (``True`` = this checkout's own ``.measurements/``, ``False`` = the staged
    evidence estate)."""

    records: list[tuple[Path, dict[str, Any]]]
    anchor: str
    local: bool


def _read_sinks(measurements: Path) -> list[tuple[Path, dict[str, Any]]]:
    """(sink path, record) pairs for every law-stamped record under
    *measurements*.

    Pre-law records are HISTORY (the append-only law preserves them); they
    carry no receipt citations and the guard demands none of them — the law's
    records are the live claims.
    """
    out: list[tuple[Path, dict[str, Any]]] = []
    for name in _SINKS:
        sink = measurements / name
        if not sink.is_file():
            continue
        for line in sink.read_text().splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # The sinks' pre-law residue (the old write_text format's
                # tail — a JSON array with merge-conflict markers baked
                # in — survives on the first lines of the append-only
                # files): history, never a law record. The law's records
                # are the ones this guard verifies.
                continue
            if record.get("law") == REDLOG_LAW:
                out.append((sink, record))
    return out


def _sink_records() -> _SinkScan:
    """The law-stamped records the guard verifies, and the estate they came
    from.

    The sinks' root resolves local-first, the evidence branch as the fallback:
    the local ``.measurements/`` when it carries a law-stamped record (a battery
    run on this tree appends to the LOCAL sinks — they are the live tree's own
    records, anchored at THIS tree's head), else — a fresh clone where the
    extraction left no local estate — the evidence branch's checkout staged
    through :func:`tests._evidence.evidence_measurements_dir`, anchored at the
    ESTATE REF'S own head: the records' home tree. A root absent locally must
    not vacate the guard, and the checkout's head is a different lineage from
    the records' — anchoring at it would history every estate record.

    What selects the local estate is a READABLE LAW RECORD, never the directory
    and not even the sink file: ``.measurements/`` is gitignored and several
    writers in this suite (the mermaid goldens, a band artifact, the in-repo
    refusal pins) create it without touching ``_SINKS``, and a bare directory
    check let any of them vacate the guard to zero records — the verdict then
    depended on pytest's ordering and on whether another worker was mid-flush.
    """
    local = _read_sinks(MEASUREMENTS)
    if local:
        return _SinkScan(local, _head(), local=True)
    from tests._evidence import evidence_measurements_dir, evidence_ref

    staged = evidence_measurements_dir()
    return _SinkScan(_read_sinks(staged), evidence_ref(), local=False)


def _is_live(record: dict[str, Any], anchor: str) -> bool:
    """Whether *record* was stamped on the *anchor* tree, or on a
    measurement-only delta of it (the off-by-one rule, the head-stamp
    verifier's own)."""
    claimed = record.get("head_sha")
    return isinstance(claimed, str) and (
        claimed == anchor or not source_changes_since(claimed, anchor=anchor)
    )


def _head_batches(
    records: list[tuple[Path, dict[str, Any]]],
) -> dict[str, list[tuple[Path, dict[str, Any]]]]:
    """The records grouped by their claimed head stamp: one battery's records
    are the BATCH its head identifies, across every sink."""
    batches: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for sink, record in records:
        claimed = record.get("head_sha")
        if isinstance(claimed, str):
            batches.setdefault(claimed, []).append((sink, record))
    return batches


def _appended_batches(
    records: list[tuple[Path, dict[str, Any]]], *, skip: frozenset[str] = frozenset()
) -> tuple[dict[str, list[tuple[Path, dict[str, Any]]]], set[str]]:
    """The batches a sink's APPEND ORDER makes current, and the skipped heads
    those selections step over.

    The sinks are append-only, so within one sink LINE ORDER IS RECENCY: a
    record written below another supersedes it, whatever ``ts`` its author
    stamped — a battery on a host whose clock lags appends an older timestamp
    after newer evidence, and a clock cannot establish append order the way the
    file's own order does.

    Across sinks no total order exists (a partial run appends to one sink), so
    each sink's own newest record is current: at most one batch per sink,
    deduplicated by head. Every batch appended after the selected one in that
    sink is superseded evidence, skipped or not; a head in *skip* is stepped
    over only while it still sits between its sink's tail and that selection.
    """
    by_sink: dict[Path, list[dict[str, Any]]] = {}
    for sink, record in records:
        by_sink.setdefault(sink, []).append(record)
    current: set[str] = set()
    stepped_over: set[str] = set()
    for sink_records in by_sink.values():
        for record in reversed(sink_records):
            claimed = record.get("head_sha")
            if not isinstance(claimed, str):
                continue
            if claimed in skip:
                stepped_over.add(claimed)
                continue
            current.add(claimed)
            break
    batches = _head_batches(records)
    return {head: batches[head] for head in current if head in batches}, stepped_over


def _missing_captures(measurements: Path, batch: list[tuple[Path, dict[str, Any]]]) -> int:
    """How many of a batch's cited captures the ESTATE ITSELF does not carry.

    The grandfather rationale is keyed to this count: an unpinned batch's
    captures stayed in an ephemeral temp root, so the estate carries none of
    them; a backfill of the estate makes them present and retires the reason.
    """
    missing = 0
    for _sink, record in batch:
        for entry in record.get("entries", []):
            capture = entry.get("capture")
            if not isinstance(capture, str) or not (measurements / capture).is_file():
                missing += 1
    return missing


def verify_receipt(record: dict[str, Any], entry: dict[str, Any], measurements: Path) -> str | None:
    """The receipt chain's verdict for one entry: ``None`` = the chain
    holds; otherwise the first broken link, named. The links:

    (a) the entry cites its scenario's run-id (the record's own run);
    (b) the entry cites the scenario's captured output file path;
    (c) the file EXISTS, its sha256 matches, and its content contains
        the recorded red (the capture's own observed, byte-equal);
    (d) the capture's own payload self-identifies — its ``run`` and
        ``pin`` fields name the record's run and the entry's pin — so a
        capture from another run or another pin cannot stand as this
        entry's receipt, whatever its citation claims.
    """
    capture_rel = entry.get("capture")
    if not capture_rel:
        return (
            f"pin={entry.get('pin')!r}: the entry cites NO capture — the "
            "provenance law's link (b) is missing (a failure log entry without "
            "a cited capture doesn't exist)"
        )
    if entry.get("run") != record.get("run"):
        return (
            f"pin={entry.get('pin')!r}: the entry's run-id {entry.get('run')!r} "
            f"is not the record's run {record.get('run')!r} — the citation's "
            "link (a) is broken (cross-run identity confusion)"
        )
    try:
        capture, homes, refused_roots = resolve_redlog_capture(
            measurements, capture_rel, record.get("captures_root")
        )
    except ValueError as refusal:
        # The refused-citation face is this entry's own violation, not a
        # run-erroring traceback: the citation and the face, named.
        return f"pin={entry.get('pin')!r}: the cited capture {capture_rel!r} is REFUSED: {refusal}"
    if not capture.is_file():
        refused_note = "".join(
            f"; REFUSED the record's own captures_root {r} — it lies inside "
            "the guard's own checkout, and a tree under review cannot ship "
            "the receipt that clears it"
            for r in refused_roots
        )
        return (
            f"pin={entry.get('pin')!r}: the cited capture {capture_rel} does "
            f"not EXIST (searched: {'; '.join(homes)}{refused_note}) — the "
            "receipt chain's link (c) is broken (a red that never ran, or a "
            "capture deleted since)"
        )
    digest = sha256(capture.read_bytes()).hexdigest()
    if digest != entry.get("sha256"):
        return (
            f"pin={entry.get('pin')!r}: the cited capture {capture_rel} does not "
            "match the recorded sha256 — the capture MUTATED since the entry "
            "recorded it (the receipt chain's link (c) is broken)"
        )
    recorded = json.loads(entry["red"])
    captured = json.loads(capture.read_text())
    if captured.get("run") != record.get("run"):
        return (
            f"pin={entry.get('pin')!r}: the cited capture's OWN run field "
            f"reads {captured.get('run')!r} but the record's run is "
            f"{record.get('run')!r} — the capture was written by another run "
            "(its content forged or its citation stolen): the receipt's "
            "identity link is broken"
        )
    if captured.get("pin") != entry.get("pin"):
        return (
            f"pin={entry.get('pin')!r}: the cited capture's OWN pin field "
            f"reads {captured.get('pin')!r} — the capture was written for "
            "another pin (its citation stolen): the receipt's identity "
            "link is broken"
        )
    if captured.get("observed") != recorded:
        return (
            f"pin={entry.get('pin')!r}: the cited capture's observed payload is "
            f"{captured.get('observed')!r} but the entry recorded {recorded!r} — "
            "the file does not contain the recorded red (the citation's "
            "content check failed)"
        )
    return None


def test_every_law_stamped_redlog_entry_cites_a_live_capture() -> None:
    """The provenance law, scoped by the head-stamp law: every entry a
    law-stamped run recorded on a VERIFIED tree cites its scenario's run-id +
    captured output file, the file exists, its digest matches, and its content
    IS the recorded red. A broken chain = the guard FAILS, the entry named.

    What a green run proves depends on the estate the sinks resolve from, and
    the non-vacuity verdict names it:

    * LOCAL mode (this checkout's ``.measurements/`` carries a readable law
      record): a battery ran on THIS tree — every record live at this head (or a
      measurement-only delta of it, the off-by-one rule) is verified in full
      against its own captures. A local estate with no live record FAILS
      CLOSED: a forgotten or stale battery re-run must not masquerade as
      verification.
    * STAGED mode (no local law record — a fresh clone): the evidence branch's
      records are verified — the ones live at the estate ref's own head, or,
      because that branch is a separate lineage whose tip descends from no
      post-extraction main commit, the newest batch each sink's append order
      makes current (never one an author's clock merely claims is newest).
      That is receipt-chain INTEGRITY, not ``HEAD``-freshness of the evidence
      branch; the latter is ``scripts/verify_evidence_heads.py``'s main-only job.

    A record stamped at a STALE head (source moved past it) is HISTORY — the
    head-stamp law's own verdict ("a capture against a stale head is history,
    never the live claim") — never deleted (the sinks are append-only, zero
    rows dropped), never demanded a file the tree no longer carries. The shape
    the scoping answers: the batteries' scenario runs append their rows to the
    TRACKED sinks while the captures land under the gitignored captures' home —
    a carry that ships the sinks without the captures leaves rows whose
    receipts died with the ephemeral worktrees. The rows are history; the LAW's
    teeth are unchanged: every verified record's chain is fully re-verified
    here, and the scenarios' findings re-derive on every battery (the
    behavioral pins — a mutation's red whose fix shipped flips a green test,
    the receipt's real half)."""
    scan = _sink_records()
    live = [(sink, record) for sink, record in scan.records if _is_live(record, scan.anchor)]
    if scan.local:
        stale_sinks = sorted(
            {sink.name for sink, record in scan.records if not _is_live(record, scan.anchor)}
        )
        assert live, (
            "THE FAILURE LOG PROVENANCE LAW'S LOCAL ESTATE: the local sinks under "
            f"{MEASUREMENTS} carry {len(scan.records)} law-stamped record(s) and NOT "
            f"ONE is live at this checkout's head {scan.anchor[:12]} (stale: "
            f"{', '.join(stale_sinks) or 'none named'}) — the guard verified "
            "nothing. Re-run the local workflow battery on HEAD to stamp fresh "
            "receipts, or remove only the named stale sink file(s) under "
            ".measurements/ (never all of .measurements/, which holds unrelated "
            "gitignored artifacts) to verify against the staged evidence estate "
            "instead."
        )
        verified = live
        scope = f"live at this checkout's head {scan.anchor[:12]}"
    elif live:
        verified = live
        scope = f"live at the evidence ref {scan.anchor[:12]}"
    else:
        batches, stepped_over = _appended_batches(
            scan.records, skip=frozenset(_GRANDFATHERED_UNPINNED_RESTSAMPS)
        )
        assert batches, (
            "THE FAILURE LOG PROVENANCE LAW'S NON-VACUITY: the staged evidence "
            f"estate at {scan.anchor[:12]} carries {len(scan.records)} law-stamped "
            "record(s) and no verifiable head batch among the monitored sinks "
            f"({', '.join(_SINKS)}) — the guard verified nothing."
        )
        heads = sorted(batches)
        verified = [record for head in heads for record in batches[head]]
        scope = "the staged estate's newest appended batch(es) " + ", ".join(
            head[:12] for head in heads
        )
        if stepped_over:
            warnings.warn(
                "the redlog guard is verifying the staged evidence estate's newest "
                f"appended batch(es) {', '.join(head[:12] for head in heads)} and is "
                "SKIPPING the grandfathered "
                f"unpinned re-stamp(s) {', '.join(sorted(sha[:12] for sha in stepped_over))}: "
                "their cited captures were never committed to the estate, so no "
                "tree can clear their receipts. Run the battery with "
                f"{REDLOG_CAPTURES_DIR_ENV}=.measurements/redlog-captures and push "
                "the captures to the evidence branch to retire the skip.",
                UserWarning,
                stacklevel=2,
            )
    violations: list[str] = []
    for sink, record in verified:
        # The receipts resolve against the same root the sink was read
        # from (the capture paths are relative to the measurements root;
        # the evidence-branch fallback stages its own copy).
        measurements = sink.parent
        for entry in record.get("entries", []):
            broken = verify_receipt(record, entry, measurements)
            if broken is not None:
                violations.append(f"{sink.name}: {broken}")
    assert not violations, (
        "THE FAILURE LOG PROVENANCE LAW: a failure log entry cites its receipt chain "
        "(run-id + capture file + digest) or it doesn't exist — these "
        "entries' chains are broken:\n" + "\n".join(violations)
    )
    entries = sum(len(record.get("entries", [])) for _sink, record in verified)
    assert verified and entries > 0, (
        "THE FAILURE LOG PROVENANCE LAW'S NON-VACUITY: "
        f"{len(verified)} record(s) / {entries} entr(ies) {scope} — the guard "
        "verified nothing, and a guard that verifies nothing has no verdict."
    )


def test_no_grandfathered_unpinned_restamp_outlives_its_reason() -> None:
    """The grandfather mapping's stale-entry pin, the ``_UV_RUN_EXCEPTIONS`` /
    ``_NARROWER_BY_DESIGN`` shape: an exception is only honest while its reason
    holds, so an entry that has outlived its reason must be removed.

    It always inspects the STAGED evidence estate — even when local sinks exist
    and the guard itself never consults the mapping — so the mapping cannot rot
    behind a developer's local ``.measurements/``. A grandfathered entry is
    STALE when

    * a newer non-grandfathered batch was appended after it in every sink that
      carries the head (the guard now verifies past that head), or
    * the grandfathered head's cited captures now exist in the estate (a
      backfill: its receipts verify, so it needs no exception), or
    * the estate no longer carries the head's records at all (the entry
      grandfathers nothing).

    While the mapping is active the pin also emits the accepted risk as a
    visible warning, so every run of this module names the skipped heads.
    """
    from tests._evidence import evidence_measurements_dir

    measurements = evidence_measurements_dir()
    records = _read_sinks(measurements)
    batches = _head_batches(records)
    _current, stepped_over = _appended_batches(
        records, skip=frozenset(_GRANDFATHERED_UNPINNED_RESTSAMPS)
    )
    stale: list[str] = []
    active: list[str] = []
    for head, why in _GRANDFATHERED_UNPINNED_RESTSAMPS.items():
        batch = batches.get(head)
        if batch is None:
            stale.append(
                f"{head[:12]}: the staged estate carries no records for this head, "
                f"so the entry grandfathers nothing — {why}"
            )
            continue
        if head not in stepped_over:
            stale.append(
                f"{head[:12]}: a newer verifiable batch is already appended after "
                f"every record of this head in each sink that carries it, so the "
                f"guard verifies past it — {why}"
            )
            continue
        if _missing_captures(measurements, batch) == 0:
            stale.append(
                f"{head[:12]}: its cited captures ARE in the staged estate now, so "
                f"its receipts verify and it needs no grandfathering — {why}"
            )
            continue
        active.append(head)
    assert not stale, (
        "THE GRANDFATHERED UNPINNED RE-STAMPS OUTLIVED THEIR REASON — the skip "
        "list must shrink, never linger:\n" + "\n".join(stale)
    )
    if active:
        warnings.warn(
            "the redlog guard grandfathered "
            f"{', '.join(sha[:12] for sha in sorted(active))} out of its staged-estate "
            "verification: those batches were re-stamped with the captures pin unset, "
            "so their receipts cite an ephemeral temp root the estate never carried. "
            "The rest of the estate is still verified.",
            UserWarning,
            stacklevel=2,
        )


def test_the_captures_are_not_tracked_in_the_product_repo() -> None:
    """The repo carries code, tests, and docs; the measurement artifacts
    (including ``.measurements/redlog-captures/``) live on the evidence
    branch, not in this index. This pin holds that: ``git ls-files``
    must list nothing under the captures' home."""
    listed = subprocess.run(
        ["git", "ls-files", "--", ".measurements/redlog-captures"],
        cwd=MEASUREMENTS.parent,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert not listed.strip(), (
        "THE REDLOG CAPTURES ARE TRACKED IN THE PRODUCT REPO: "
        f"{len(listed.split())} paths under .measurements/redlog-captures/ "
        "ride the index. The measurement artifacts belong to the evidence "
        "branch — remove them from the index "
        "(git rm -r --cached .measurements/redlog-captures)."
    )


def test_the_guard_rejects_the_two_shape_bypasses_by_provenance(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The shape rule's TWO LIVE BYPASSES, re-tested against the
    provenance rule: NEITHER form escapes the receipt chain.

    * the KEYWORD-FORM payload (the old scan read ``args[2]`` and
      skipped) — records like any call now, its entry VERIFIED by
      receipt; break the receipt (delete the capture) and the guard
      rejects the entry, whatever syntax produced it;
    * the NAME-LAUNDERED CONSTANT (the old scan saw an ``ast.Name`` and
      passed) — same: the entry is verified by its CAPTURE's content,
      not by resolving the name's grammar; mutate the capture behind the
      digest and the guard rejects (the content check).
    """
    from tests._wf_fixtures import RedLog

    sink = tmp_path / "bypass-reds.json"
    monkeypatch.setattr("tests._wf_fixtures.MEASUREMENTS", tmp_path)
    # The captures' root is pinned to the test's own directory: both the
    # sink file and the cited captures resolve inside tmp_path.
    monkeypatch.setenv(REDLOG_CAPTURES_DIR_ENV, str(tmp_path / "captures"))
    measurements_root = tmp_path  # the RedLog module-global is patched above

    log = RedLog(sink.name)

    # bypass 1, the keyword form — the old guard's blind spot:
    log.red("bypass-keyword-form", "the payload rides observed=", observed={"zombie_wake": True})
    # bypass 2, the name-laundered constant — the old guard saw a Name:
    fabricated_constant = {"zombie_wake": True, "note": "never exercised"}
    log.red("bypass-laundered-constant", "the payload is a module constant", fabricated_constant)
    log.flush()

    records = [json.loads(line) for line in sink.read_text().splitlines()]
    assert len(records) == 1 and records[0]["law"] == REDLOG_LAW
    for entry in records[0]["entries"]:
        # The provenance verdict: both entries' receipts hold (each cites
        # a live, digest-matching, content-matching capture) — the
        # verdict is by the chain, never the call's shape.
        assert verify_receipt(records[0], entry, measurements_root) is None, entry

    # Both rejections, by provenance:
    keyword_entry, laundered_entry = records[0]["entries"]
    # 1. The keyword-form entry whose capture vanished: rejected.
    capture_of = lambda entry: resolve_redlog_capture(  # noqa: E731
        measurements_root, entry["capture"], records[0].get("captures_root")
    )[0]
    capture_of(keyword_entry).unlink()
    broken = verify_receipt(records[0], keyword_entry, measurements_root)
    assert broken is not None and "does not EXIST" in broken, broken
    # 2. The laundered-constant entry whose capture mismatches its
    #    recorded red (the capture rewritten behind the digest — the
    #    stale/mutated receipt): rejected.
    capture_path = capture_of(laundered_entry)
    capture_path.write_text(
        json.dumps(
            {
                "pin": laundered_entry["pin"],
                "mutation": laundered_entry["mutation"],
                "observed": {"zombie_wake": False},
                "run": records[0]["run"],
                "ts": "1970-01-01T00:00:00",
            },
            indent=2,
            sort_keys=True,
        )
    )
    broken = verify_receipt(records[0], laundered_entry, measurements_root)
    assert broken is not None and (
        "does not match the recorded sha256" in broken or "content check failed" in broken
    ), broken


def test_a_fabricated_sink_entry_without_a_citation_is_rejected(tmp_path: Path) -> None:
    """The hand-edited sink (the fabrication vector the shape rule could
    never see — it lived outside the source): an entry appended to a
    sink without its receipt chain is rejected at guard time. The red
    that was never run doesn't exist."""
    fabricated = {
        "run": "19700101T000000-0-deadbeef",
        "ts": "1970-01-01T00:00:00",
        "law": REDLOG_LAW,
        "entries": [
            {
                "pin": "fabricated-without-a-scenario",
                "mutation": "nothing was mutated",
                "red": '{"zombie_wake": true}',
            }
        ],
    }
    record = {"run": fabricated["run"], "law": REDLOG_LAW, "entries": fabricated["entries"]}
    broken = verify_receipt(record, record["entries"][0], tmp_path)
    assert broken is not None and "cites NO capture" in broken, broken


def test_the_captures_default_outside_the_repo_tree(tmp_path: Path, monkeypatch: Any) -> None:
    """With no pin set, a scenario's capture file is written under the
    system temp root and never into the repository tree — a pytest run
    leaves no untracked per-run capture directories behind."""
    monkeypatch.delenv(REDLOG_CAPTURES_DIR_ENV, raising=False)
    log = RedLog("default-root-reds.json")
    log.red("default-root-pin", "no mutation", {"ok": True})

    capture, _, _ = resolve_redlog_capture(MEASUREMENTS, log.entries[0]["capture"])
    assert capture.is_file()
    assert capture.resolve().is_relative_to(redlog_captures_root().resolve())
    assert not capture.resolve().is_relative_to(MEASUREMENTS.resolve())


def test_an_explicit_pin_places_the_captures_under_the_pinned_root(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """With ``TASKQ_REDLG_CAPTURES_DIR`` set, the capture lands under
    that directory — the battery recording estate evidence keeps its
    in-repo output — and the citation resolves to it."""
    pinned_root = tmp_path / "pinned-captures"
    monkeypatch.setenv(REDLOG_CAPTURES_DIR_ENV, str(pinned_root))
    log = RedLog("pinned-root-reds.json")
    log.red("pinned-root-pin", "no mutation", {"ok": True})

    capture, _, _ = resolve_redlog_capture(MEASUREMENTS, log.entries[0]["capture"])
    assert capture.is_file()
    assert capture.resolve().is_relative_to(pinned_root.resolve())
    assert redlog_captures_root() == pinned_root


def test_the_root_pin_resolves_absolute(tmp_path: Path, monkeypatch: Any) -> None:
    """A relative pin is stored resolved to an absolute path — a reader
    in another working directory resolves the same directory, and the
    record's captures root is usable verbatim."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(REDLOG_CAPTURES_DIR_ENV, "captures-rel")
    root = redlog_captures_root()
    assert root.is_absolute()
    assert root == (tmp_path / "captures-rel").resolve()


def test_the_default_root_is_per_uid_and_private(tmp_path: Path, monkeypatch: Any) -> None:
    """The default root is namespaced per uid and created 0700: the
    captures are receipts, and a shared-world-readable temp directory
    is neither."""
    monkeypatch.delenv(REDLOG_CAPTURES_DIR_ENV, raising=False)
    log = RedLog("private-root-reds.json")
    log.red("private-root-pin", "no mutation", {"ok": True})

    root = redlog_captures_root()
    assert f"-{os.getuid()}" in root.name
    mode = stat.S_IMODE(root.stat().st_mode)
    assert mode == 0o700, f"the captures root is mode {oct(mode)}, not 0700"


def _forged_entry(
    tmp_path: Path, captures_root: Path, payload_pin: str, payload_run: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """A record + entry pair whose cited capture is REAL (on disk, with
    a matching sha256 and a matching observed payload) but whose
    payload's own identity fields name *payload_pin* / *payload_run*
    instead of the run the record claims — the self-consistent forgery
    the receipt chain's identity links must catch."""
    observed = {"ok": True}
    capture_dir = captures_root / _FORGED_RUN
    capture_dir.mkdir(parents=True, exist_ok=True)
    capture = capture_dir / "pin-x.json"
    payload = {
        "pin": payload_pin,
        "mutation": "no mutation",
        "observed": observed,
        "run": payload_run,
        "ts": "2026-01-01T00:00:00",
    }
    capture.write_text(json.dumps(payload, indent=2, sort_keys=True))
    entry = {
        "pin": "pin-x",
        "mutation": "no mutation",
        "red": json.dumps(observed),
        "run": _FORGED_RUN,
        "capture": f"redlog-captures/{_FORGED_RUN}/pin-x.json",
        "sha256": sha256(capture.read_bytes()).hexdigest(),
    }
    record = {
        "run": _FORGED_RUN,
        "ts": "2026-01-01T00:00:00",
        "law": REDLOG_LAW,
        "captures_root": str(captures_root),
        "entries": [entry],
    }
    return record, entry


def test_a_capture_written_by_another_run_is_rejected(tmp_path: Path) -> None:
    """A cited capture whose own payload names another run does not
    prove this run's red — the identity link is checked from the
    capture's content, not the citation's say-so."""
    record, entry = _forged_entry(
        tmp_path, tmp_path / "caps", "pin-x", "20260101T000000-2-beefbeef"
    )
    broken = verify_receipt(record, entry, tmp_path)
    assert broken is not None and "another run" in broken, broken


def test_a_capture_written_for_another_pin_is_rejected(tmp_path: Path) -> None:
    """A cited capture whose own payload names another pin does not
    prove this pin's red — the capture must self-identify with the
    entry's pin."""
    record, entry = _forged_entry(
        tmp_path, tmp_path / "caps", "pin-y", "20260101T000000-1-cafecafe"
    )
    broken = verify_receipt(record, entry, tmp_path)
    assert broken is not None and "another pin" in broken, broken


def test_an_escaping_citation_is_refused(tmp_path: Path) -> None:
    """A citation that is absolute or walks out with ``..`` names a file
    outside the estate — refused as its own entry's violation (a receipt
    line naming the citation and the face), not an erroring run."""
    record, entry = _forged_entry(tmp_path, tmp_path / "caps", "pin-x", _FORGED_RUN)
    for forged in ("/etc/hostname", "redlog-captures/../../outside.json"):
        escaped = dict(entry, capture=forged)
        broken = verify_receipt(record, escaped, tmp_path)
        assert broken is not None, broken
        assert "REFUSED" in broken and "estate-relative" in broken, broken


def _in_repo_capture_root(tmp_path: Path) -> Path:
    """A test-private capture root inside the repository, beside rather than
    under the shared ``.measurements/`` estate. The unique leaf preserves the
    in-repo refusal without teardown racing a concurrent estate writer."""
    return MEASUREMENTS.parent / f".forge-caps-{tmp_path.name}"


def _remove_in_repo_capture_root(capture_root: Path) -> None:
    """Remove only this test's unique leaf; never remove a shared parent."""
    shutil.rmtree(capture_root, ignore_errors=True)


def test_a_record_root_inside_the_repo_is_refused(tmp_path: Path) -> None:
    """A record whose own captures_root names a path inside the
    repository containing the record cannot clear its receipt with a
    capture the tree ships itself: that root is refused, and the
    verdict is the missing-capture refusal naming it."""
    capture_root = _in_repo_capture_root(tmp_path)
    record, entry = _forged_entry(tmp_path, capture_root, "pin-x", _FORGED_RUN)
    try:
        broken = verify_receipt(record, entry, MEASUREMENTS)
        assert broken is not None, (
            f"the forged receipt passed: the record's captures_root "
            f"{capture_root} lies inside the repository containing the "
            "record — a tree under review cannot ship the receipt that "
            "clears it"
        )
        assert "REFUSED" in broken and str(capture_root) in broken, broken
    finally:
        _remove_in_repo_capture_root(capture_root)


def test_a_cwd_relative_record_root_inside_the_repo_is_refused(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Same refusal for a record-declared root given as a relative path:
    resolved against the reader's working directory it lands inside the
    repository containing the record, and the resolution skips it."""
    monkeypatch.chdir(MEASUREMENTS.parent)
    capture_root = _in_repo_capture_root(tmp_path)
    record, entry = _forged_entry(tmp_path, capture_root, "pin-x", _FORGED_RUN)
    record["captures_root"] = f"./{capture_root.name}"
    try:
        broken = verify_receipt(record, entry, MEASUREMENTS)
        assert broken is not None, (
            f"the forged receipt passed: the record's relative captures_root "
            f"{record['captures_root']!r} resolves inside the repository "
            "containing the record"
        )
        assert "REFUSED" in broken and str(capture_root) in broken, broken
    finally:
        _remove_in_repo_capture_root(capture_root)


def test_a_record_root_outside_the_repo_still_resolves(tmp_path: Path) -> None:
    """The trust rule's legitimate case (d): a battery run from a
    checkout whose captures went to a directory OUTSIDE the repository —
    its record-declared root is honored and the receipt chain resolves."""
    external = tmp_path / "external-caps"
    record, entry = _forged_entry(tmp_path, external, "pin-x", _FORGED_RUN)
    broken = verify_receipt(record, entry, MEASUREMENTS)
    assert broken is None, broken


def _staged_measurements() -> Path:
    """The STAGED estate (the guard's fallback face: no local
    ``.measurements``, the CI shape) — the evidence branch's copy."""
    from tests import _evidence

    return _evidence.evidence_measurements_dir()


def test_a_record_root_inside_the_repo_is_refused_on_a_staged_estate(
    tmp_path: Path,
) -> None:
    """The in-repo refusal holds when the estate is STAGED (no local
    ``.measurements``): the record's captures_root names the repository
    the GUARD runs from, and the receipt must not clear against a
    capture that tree ships itself."""
    capture_root = _in_repo_capture_root(tmp_path)
    record, entry = _forged_entry(tmp_path, capture_root, "pin-x", _FORGED_RUN)
    staged = _staged_measurements()
    try:
        broken = verify_receipt(record, entry, staged)
        assert broken is not None, (
            f"the forged receipt passed on a staged estate: the record's "
            f"captures_root {capture_root} lies inside the guard's own "
            "checkout — the anchor for the refusal is the checkout, not "
            "the estate's location"
        )
        assert "REFUSED" in broken and str(capture_root) in broken, broken
    finally:
        _remove_in_repo_capture_root(capture_root)


def test_a_record_root_outside_the_repo_resolves_on_a_staged_estate(
    tmp_path: Path,
) -> None:
    """The staged estate keeps the legitimate case: a record whose
    captures went to an external directory (the cross-TMPDIR chain's
    shape) resolves through its record-declared root."""
    external = tmp_path / "external-caps"
    record, entry = _forged_entry(tmp_path, external, "pin-x", _FORGED_RUN)
    staged = _staged_measurements()
    broken = verify_receipt(record, entry, staged)
    assert broken is None, broken


def test_the_anchor_survives_a_symlinked_checkout(tmp_path: Path) -> None:
    """The trust rule's anchor is the checkout's REAL path: a symlinked
    view of the checkout must not become the anchor, or a record root
    resolved to the real tree escapes it and the in-repo refusal slips."""
    link = tmp_path / "linked-checkout"
    link.symlink_to(_REPO)
    proc = subprocess.run(  # Why: the interpreter is sys.executable; fixed argv, no shell.
        [sys.executable, "-c", "from tests import _wf_fixtures as f; print(f._REPO_ROOT)"],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,  # no tests/ here: the import can only resolve through the link
        env={**os.environ, "PYTHONPATH": f"{link}:{_REPO / 'src'}"},
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    anchor = Path(proc.stdout.strip())
    assert anchor == _REPO_ROOT, (
        f"the trust rule's anchor is {anchor} — the symlinked view's walk. "
        f"The record's declared root resolves to the real tree "
        f"({_REPO_ROOT}), so a root inside the checkout escapes the "
        "unresolved anchor and the refusal never fires: the anchor must be "
        "resolved before the walk"
    )


def test_a_missing_capture_names_the_home_it_searched(tmp_path: Path) -> None:
    """The missing-capture verdict names the resolved home it searched —
    the reader's answer to 'deleted since, or never on this machine?'."""
    record, entry = _forged_entry(tmp_path, tmp_path / "caps", "pin-x", _FORGED_RUN)
    record_root = Path(record["captures_root"]) / entry["capture"].removeprefix("redlog-captures/")
    record_root.unlink()
    broken = verify_receipt(record, entry, tmp_path)
    assert broken is not None, broken
    assert "does not EXIST" in broken and "searched:" in broken, broken
    assert str(tmp_path / "caps") in broken, broken  # the record's own root, named


def test_source_changes_since_treats_non_ancestor_head_as_stale(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A claimed head that is not an ancestor of the anchor is stale by
    definition even when the two-dot tree diff outside ``.measurements/``
    is empty (sibling/unrelated lineage sharing the same source tree)."""
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "tests").mkdir(parents=True)
    (repo / "src" / "app.py").write_text("X = 1\n", encoding="utf-8")

    git_env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
    }

    def _git(*args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
            env=git_env,
        ).stdout.strip()

    _git("init", "-q")
    _git("config", "user.name", "pins")
    _git("config", "user.email", "pins@localhost")
    _git("config", "commit.gpgsign", "false")
    _git("add", "-A")
    _git("commit", "-q", "-m", "main base")
    main_head = _git("rev-parse", "HEAD")

    # Positive control: a measurement-only child commit on the same lineage
    # keeps the ancestor fresh.
    (repo / ".measurements").mkdir()
    (repo / ".measurements" / "note.txt").write_text("m1\n", encoding="utf-8")
    _git("add", "-A")
    _git("commit", "-q", "-m", "measurements only")
    descendant_head = _git("rev-parse", "HEAD")

    # Unrelated lineage (orphan branch) carrying the exact same source tree
    # outside .measurements/ so two-dot `git diff main_head..orphan_head`
    # with exclusions is empty, yet `main_head` is NOT an ancestor.
    _git("checkout", "--orphan", "evidence-lineage", "-q")
    (repo / ".measurements" / "note.txt").write_text("m2\n", encoding="utf-8")
    _git("add", "-A")
    _git("commit", "-q", "-m", "orphan estate with identical source tree")
    orphan_head = _git("rev-parse", "HEAD")

    monkeypatch.setattr("tests._wf_fixtures.__file__", str(repo / "tests" / "_wf_fixtures.py"))
    monkeypatch.setattr("tests._wf_fixtures._REPO_ROOT", repo.resolve())
    monkeypatch.setattr("tests._wf_fixtures._SOURCE_CHANGES_MEMO", {})

    assert source_changes_since(main_head, anchor=descendant_head) is False, (
        "measurement-only delta from a true ancestor must remain fresh"
    )
    assert source_changes_since(main_head, anchor=orphan_head) is True, (
        f"claimed head {main_head[:12]} is NOT an ancestor of anchor {orphan_head[:12]} "
        "yet source_changes_since treated it as fresh because the two-dot tree diff "
        "ignored ancestry"
    )
    assert source_changes_since("f" * 40, anchor=descendant_head) is True, (
        "an unresolvable claimed head must fail closed as stale"
    )


def test_forge_caps_refusal_tests_do_not_leave_measurements_dir_behind(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The in-repo refusal tests leave neither ``.measurements/`` nor
    their test-private capture roots behind."""
    fake_repo = tmp_path / "repo"
    fake_repo.mkdir()
    fake_measurements = fake_repo / ".measurements"
    staged = tmp_path / "staged"
    staged.mkdir()

    def _fake_staged() -> Path:
        return staged

    monkeypatch.setattr("tests.test_fv_redlog_guard.MEASUREMENTS", fake_measurements)
    monkeypatch.setattr("tests._wf_fixtures._REPO_ROOT", fake_repo.resolve())
    monkeypatch.setattr("tests.test_fv_redlog_guard._staged_measurements", _fake_staged)

    cases = [
        (
            "test_a_record_root_inside_the_repo_is_refused",
            lambda: test_a_record_root_inside_the_repo_is_refused(tmp_path),
        ),
        (
            "test_a_cwd_relative_record_root_inside_the_repo_is_refused",
            lambda: test_a_cwd_relative_record_root_inside_the_repo_is_refused(
                tmp_path, monkeypatch
            ),
        ),
        (
            "test_a_record_root_inside_the_repo_is_refused_on_a_staged_estate",
            lambda: test_a_record_root_inside_the_repo_is_refused_on_a_staged_estate(tmp_path),
        ),
    ]
    leaked: list[str] = []
    for name, fn in cases:
        if fake_measurements.exists():
            shutil.rmtree(fake_measurements)
        fn()
        artifacts = [fake_measurements, *fake_repo.glob(".forge-caps-*")]
        existing = [path for path in artifacts if path.exists()]
        if existing:
            leaked.append(f"{name}: {[path.name for path in existing]}")
            for path in existing:
                shutil.rmtree(path)

    assert not leaked, (
        f"in-repo forge-caps refusal tests left repository artifacts behind after cleanup: {leaked}"
    )


def test_sink_records_does_not_vacate_on_sinkless_measurements_dir(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A local ``.measurements/`` directory containing no redlog sink files
    (e.g. an empty dir or unrelated artifacts like ``t09-mermaid-golden.mmd``)
    must resolve the same records as an absent ``.measurements/`` directory
    rather than vacating the guard with zero records."""
    staged = tmp_path / "staged"
    staged.mkdir()
    synthetic_record = {
        "run": _FORGED_RUN,
        "ts": "2026-01-01T00:00:00",
        "law": REDLOG_LAW,
        "head_sha": "a" * 40,
        "entries": [],
    }
    (staged / _SINKS[0]).write_text(json.dumps(synthetic_record) + "\n", encoding="utf-8")

    def _fake_measurements_dir() -> Path:
        return staged

    def _fake_evidence_ref() -> str:
        return "a" * 40

    monkeypatch.setattr("tests._evidence.evidence_measurements_dir", _fake_measurements_dir)
    monkeypatch.setattr("tests._evidence.evidence_ref", _fake_evidence_ref)

    absent = tmp_path / "absent-measurements"
    monkeypatch.setattr("tests.test_fv_redlog_guard.MEASUREMENTS", absent)
    absent_records = _sink_records()[0]
    assert len(absent_records) == 1

    sinkless = tmp_path / "sinkless-measurements"
    sinkless.mkdir()
    (sinkless / "t09-mermaid-golden.mmd").write_text("graph TD\n", encoding="utf-8")
    monkeypatch.setattr("tests.test_fv_redlog_guard.MEASUREMENTS", sinkless)
    sinkless_records = _sink_records()[0]
    assert len(sinkless_records) == len(absent_records), (
        "_sink_records() switched to the local .measurements/ directory merely "
        "because it existed, returning 0 records when no sink files were present "
        f"(absent={len(absent_records)}, sinkless={len(sinkless_records)})"
    )


# ── the staged fallback: which batch it verifies, and the grandfather pin ──

#: The anchor every synthetic staged estate is read against: a sha this
#: checkout cannot resolve, so no synthetic record is ever "live" and the
#: fallback's batch selection is what the pins exercise.
_SYNTHETIC_ESTATE_REF = "a" * 40


def _synthetic_record(
    measurements: Path,
    *,
    head: str,
    ts: str,
    run: str,
    pin: str = "synthetic-pin",
    declared_root: Path | None = None,
) -> dict[str, Any]:
    """A law-stamped record whose receipt chain HOLDS: its capture is written
    under the estate's own captures namespace (``measurements/redlog-
    captures/``) unless *declared_root* names the physical root the record
    cites instead (the unpinned battery's shape)."""
    physical = declared_root if declared_root is not None else measurements / "redlog-captures"
    capture_dir = physical / run
    capture_dir.mkdir(parents=True, exist_ok=True)
    capture = capture_dir / f"{pin}.json"
    observed = {"ok": True}
    capture.write_text(
        json.dumps(
            {"pin": pin, "mutation": "no mutation", "observed": observed, "run": run, "ts": ts},
            indent=2,
            sort_keys=True,
        )
    )
    record: dict[str, Any] = {
        "run": run,
        "ts": ts,
        "law": REDLOG_LAW,
        "head_sha": head,
        "entries": [
            {
                "pin": pin,
                "mutation": "no mutation",
                "red": json.dumps(observed),
                "run": run,
                "capture": f"redlog-captures/{run}/{pin}.json",
                "sha256": sha256(capture.read_bytes()).hexdigest(),
            }
        ],
    }
    if declared_root is not None:
        record["captures_root"] = str(declared_root)
    return record


def _append_record(measurements: Path, sink_name: str, record: dict[str, Any]) -> None:
    measurements.mkdir(parents=True, exist_ok=True)
    with (measurements / sink_name).open("a") as sink:
        sink.write(json.dumps(record) + "\n")


def _staged_estate(tmp_path: Path, monkeypatch: Any) -> Path:
    """Point the guard's staged fallback at a fresh, empty estate under
    *tmp_path*, and the local sinks away from this checkout — the estate a
    fresh clone reads."""
    staged = tmp_path / "staged-measurements"
    staged.mkdir()
    monkeypatch.setattr("tests._evidence.evidence_measurements_dir", lambda: staged)
    monkeypatch.setattr("tests._evidence.evidence_ref", lambda: _SYNTHETIC_ESTATE_REF)
    monkeypatch.setattr("tests.test_fv_redlog_guard.MEASUREMENTS", tmp_path / "absent-local")
    return staged


def test_the_staged_fallback_skips_only_the_grandfathered_unpinned_restamps(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The staged fallback verifies the newest batch the estate can actually
    clear, names the grandfathered heads it stepped over in a visible warning,
    and still fails on a broken receipt in the batch it selected.

    The unpinned batch is appended AFTER the verifiable one — the estate's own
    shape, where the re-stamps landed last — because append order, not the
    record-authored ``ts``, is what makes it the tail a reader must step over."""
    staged = _staged_estate(tmp_path, monkeypatch)
    grandfathered = sorted(_GRANDFATHERED_UNPINNED_RESTSAMPS)[0]
    ephemeral = tmp_path / "ephemeral-captures"
    verifiable_head = "b" * 40
    verifiable = _synthetic_record(
        staged, head=verifiable_head, ts="2026-10-12T23:39:55", run="20261012T233955-1-verif000"
    )
    for sink_name in _SINKS:
        _append_record(staged, sink_name, verifiable)
    _append_record(
        staged,
        _SINKS[0],
        _synthetic_record(
            staged,
            head=grandfathered,
            ts="2026-11-23T21:51:41",
            run="20261123T215141-1-pinned000",
            declared_root=ephemeral,
        ),
    )
    # the unpinned battery's captures never reached the estate
    for capture in ephemeral.rglob("*.json"):
        capture.unlink()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    messages = [str(warning.message) for warning in caught]
    assert any(grandfathered[:12] in message for message in messages), (
        "the staged fallback stepped over a grandfathered head without naming it — "
        f"the accepted risk must be visible in every run. Warnings: {messages}"
    )
    assert any(verifiable_head[:12] in message for message in messages), (
        f"the warning must name the batch that WAS verified. Warnings: {messages}"
    )

    # the selected batch is verified for real: a deleted capture reds it
    victim = staged / f"redlog-captures/{verifiable['run']}/synthetic-pin.json"
    victim.unlink()
    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    assert "does not EXIST" in str(failure.value), str(failure.value)


def test_a_new_unpinned_staged_batch_is_not_skipped(tmp_path: Path, monkeypatch: Any) -> None:
    """A batch outside the closed grandfather mapping is NOT skipped: the
    newest head on the estate is verified like any other, so a repeat of the
    unpinned re-stamp regression fails the guard loudly instead of sliding past
    an ever-widening filter."""
    staged = _staged_estate(tmp_path, monkeypatch)
    ephemeral = tmp_path / "ephemeral-captures"
    grandfathered = sorted(_GRANDFATHERED_UNPINNED_RESTSAMPS)[0]
    _append_record(
        staged,
        _SINKS[0],
        _synthetic_record(
            staged,
            head=grandfathered,
            ts="2026-11-09T14:31:02",
            run="20261109T143102-1-known000",
            declared_root=ephemeral,
        ),
    )
    new_head = "c" * 40
    _append_record(
        staged,
        _SINKS[0],
        _synthetic_record(
            staged,
            head=new_head,
            ts="2026-12-24T00:00:00",
            run="20261224T000000-1-newpin00",
            pin="new-unpinned-pin",
            declared_root=ephemeral,
        ),
    )
    for capture in ephemeral.rglob("*.json"):
        capture.unlink()

    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    message = str(failure.value)
    assert "does not EXIST" in message and "new-unpinned-pin" in message, message


def test_a_backdated_new_unpinned_staged_batch_is_not_skipped(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Append order, not a record-authored clock, decides whether a new batch
    hides behind older evidence. A battery on a machine whose clock is behind
    can append an old ``ts``; its missing receipts must still fail the guard."""
    staged = _staged_estate(tmp_path, monkeypatch)
    _append_record(
        staged,
        _SINKS[0],
        _synthetic_record(
            staged,
            head="b" * 40,
            ts="2026-10-12T23:39:55",
            run="20261012T233955-1-verif000",
        ),
    )
    ephemeral = tmp_path / "ephemeral-captures"
    _append_record(
        staged,
        _SINKS[0],
        _synthetic_record(
            staged,
            head="c" * 40,
            ts="2020-01-01T00:00:00",
            run="20200101T000000-1-clock000",
            pin="backdated-unpinned-pin",
            declared_root=ephemeral,
        ),
    )
    for capture in ephemeral.rglob("*.json"):
        capture.unlink()

    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    message = str(failure.value)
    assert "does not EXIST" in message and "backdated-unpinned-pin" in message, message


def test_each_sink_has_its_own_current_batch(tmp_path: Path, monkeypatch: Any) -> None:
    """A partial run appends to one sink, so two sinks' newest records can be
    different batteries — and neither is the other's history. Each sink's own
    tail is verified, so a broken receipt in the sink whose head is not the
    newest anywhere still fails the guard."""
    staged = _staged_estate(tmp_path, monkeypatch)
    ephemeral = tmp_path / "ephemeral-captures"
    _append_record(
        staged,
        _SINKS[0],
        _synthetic_record(
            staged, head="b" * 40, ts="2026-10-12T23:39:55", run="20261012T233955-1-good000"
        ),
    )
    _append_record(
        staged,
        _SINKS[1],
        _synthetic_record(
            staged,
            head="c" * 40,
            ts="2020-01-01T00:00:00",
            run="20200101T000000-1-laggin0",
            pin="second-sink-tail",
            declared_root=ephemeral,
        ),
    )
    for capture in ephemeral.rglob("*.json"):
        capture.unlink()

    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    message = str(failure.value)
    assert "does not EXIST" in message and "second-sink-tail" in message, message


def test_the_grandfather_pin_names_the_risk_and_fails_once_superseded(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The stale pin is live on today's estate (the entries are still
    justified, and the risk is emitted), and it reds on the two states that
    retire an entry: a newer verifiable batch was appended after it, or the
    entry's captures reached the estate."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        test_no_grandfathered_unpinned_restamp_outlives_its_reason()
    messages = [str(warning.message) for warning in caught]
    assert all(
        any(head[:12] in message for message in messages)
        for head in _GRANDFATHERED_UNPINNED_RESTSAMPS
    ), f"every active grandfather must be visible on every run. Warnings: {messages}"

    # 1. a newer verifiable batch supersedes the entry. Its record is appended
    # BELOW the grandfathered one while carrying an OLDER ts, so only append
    # order can retire the entry — a clock cannot.
    superseded = _staged_estate(tmp_path, monkeypatch)
    grandfathered = sorted(_GRANDFATHERED_UNPINNED_RESTSAMPS)[0]
    _append_record(
        superseded,
        _SINKS[0],
        _synthetic_record(
            superseded,
            head=grandfathered,
            ts="2026-11-09T14:31:02",
            run="20261109T143102-1-known000",
            declared_root=tmp_path / "ephemeral-captures",
        ),
    )
    _append_record(
        superseded,
        _SINKS[0],
        _synthetic_record(
            superseded, head="d" * 40, ts="2020-01-01T00:00:00", run="20200101T000000-1-later000"
        ),
    )
    with pytest.raises(AssertionError) as failure:
        test_no_grandfathered_unpinned_restamp_outlives_its_reason()
    assert "newer verifiable batch" in str(failure.value), str(failure.value)

    # 2. a backfill put the grandfathered head's captures in the estate
    backfilled = tmp_path / "backfilled-measurements"
    backfilled.mkdir()
    monkeypatch.setattr("tests._evidence.evidence_measurements_dir", lambda: backfilled)
    _append_record(
        backfilled,
        _SINKS[0],
        _synthetic_record(
            backfilled,
            head=grandfathered,
            ts="2026-11-09T14:31:02",
            run="20261109T143102-1-known000",
        ),
    )
    with pytest.raises(AssertionError) as failure:
        test_no_grandfathered_unpinned_restamp_outlives_its_reason()
    assert "ARE in the staged estate now" in str(failure.value), str(failure.value)


def test_the_guard_refuses_an_estate_with_no_law_records(tmp_path: Path, monkeypatch: Any) -> None:
    """Zero law-stamped records read is a FAILURE, never a vacuous pass: a
    renamed sink, a missing staged estate, or an emptied record set must not
    read as a clean verdict."""
    _staged_estate(tmp_path, monkeypatch)
    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    message = str(failure.value)
    assert "NON-VACUITY" in message and "verified nothing" in message, message


def test_the_guard_refuses_law_records_with_no_entries(tmp_path: Path, monkeypatch: Any) -> None:
    """A readable law record is not itself evidence: if every selected record
    has an empty entry list, the guard still verified nothing and must fail."""
    staged = _staged_estate(tmp_path, monkeypatch)
    record = _synthetic_record(
        staged,
        head="b" * 40,
        ts="2026-10-12T23:39:55",
        run="20261012T233955-1-empty000",
    )
    record["entries"] = []
    _append_record(staged, _SINKS[0], record)

    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    message = str(failure.value)
    assert "NON-VACUITY" in message and "0 entr(ies)" in message, message


def test_the_grandfather_pin_retires_an_entry_whose_head_disappears(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """An exception for a head the append-only estate no longer carries is
    dead weight. Keep the other entries active so only the absent-head trigger
    can produce this failure."""
    staged = _staged_estate(tmp_path, monkeypatch)
    omitted = sorted(_GRANDFATHERED_UNPINNED_RESTSAMPS)[0]
    for index, head in enumerate(sorted(_GRANDFATHERED_UNPINNED_RESTSAMPS)):
        if head == omitted:
            continue
        ephemeral = tmp_path / f"ephemeral-{index}"
        _append_record(
            staged,
            _SINKS[0],
            _synthetic_record(
                staged,
                head=head,
                ts=f"2026-11-{9 + index:02d}T00:00:00",
                run=f"202611{9 + index:02d}T000000-1-active{index}",
                declared_root=ephemeral,
            ),
        )
        for capture in ephemeral.rglob("*.json"):
            capture.unlink()

    with pytest.raises(AssertionError) as failure:
        test_no_grandfathered_unpinned_restamp_outlives_its_reason()
    message = str(failure.value)
    assert omitted[:12] in message and "grandfathers nothing" in message, message
    assert "newer verifiable batch" not in message, message
    assert "ARE in the staged estate now" not in message, message


def test_the_guard_verifies_live_local_receipts(tmp_path: Path, monkeypatch: Any) -> None:
    """Local mode verifies this checkout's live receipts, not merely their
    presence: deleting a selected capture turns the guard red."""
    local = tmp_path / "local-measurements"
    record = _synthetic_record(
        local, head=_head(), ts="2026-01-01T00:00:00", run="20260101T000000-1-local00"
    )
    _append_record(local, _SINKS[0], record)
    monkeypatch.setattr("tests.test_fv_redlog_guard.MEASUREMENTS", local)

    test_every_law_stamped_redlog_entry_cites_a_live_capture()

    (local / f"redlog-captures/{record['run']}/synthetic-pin.json").unlink()
    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    assert "does not EXIST" in str(failure.value), str(failure.value)


def test_the_guard_fails_closed_on_a_local_estate_with_no_live_record(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A local estate whose sinks carry only stale heads fails closed with a
    targeted remedy — it never falls back to the evidence branch, and the
    remedy names the sink files rather than the whole ``.measurements/``."""
    local = tmp_path / "local-measurements"
    _append_record(
        local,
        _SINKS[0],
        _synthetic_record(
            local, head="e" * 40, ts="2020-01-01T00:00:00", run="20200101T000000-1-stale000"
        ),
    )
    monkeypatch.setattr("tests.test_fv_redlog_guard.MEASUREMENTS", local)

    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    message = str(failure.value)
    assert _SINKS[0] in message, message
    assert "Re-run the local workflow battery" in message, message
    assert "rm -rf" not in message, message


def test_a_local_sink_without_a_law_record_falls_back_to_the_staged_estate(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """A local sink file carrying no READABLE law record — only pre-law residue,
    or a line another worker is mid-append on — is no local estate at all: the
    guard reads the staged evidence estate instead of reporting a verdict over
    zero local law records, so a concurrent flush can never turn into a red."""
    staged = _staged_estate(tmp_path, monkeypatch)
    _append_record(
        staged,
        _SINKS[0],
        _synthetic_record(
            staged, head="b" * 40, ts="2026-10-12T23:39:55", run="20261012T233955-1-verif000"
        ),
    )
    local = tmp_path / "local-measurements"
    local.mkdir()
    (local / _SINKS[0]).write_text('[{"run": "old", "entries": []}\n', encoding="utf-8")
    monkeypatch.setattr("tests.test_fv_redlog_guard.MEASUREMENTS", local)

    scan = _sink_records()
    assert scan.local is False, (
        "a sink file with no law record selected the local estate — the guard would "
        "then report a verdict over zero local records"
    )
    assert len(scan.records) == 1
    test_every_law_stamped_redlog_entry_cites_a_live_capture()


def test_staged_records_live_at_the_estate_ref_are_verified_directly(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """When a staged record is live at the estate ref's own head — the branch
    that carries the checkout's history, not the separated lineage — it is
    verified as it stands, EVEN IF its head sits in the grandfather mapping:
    the mapping bounds a fallback that cannot clear a receipt, never a record
    whose receipt is right there. A broken receipt still reds."""
    staged = _staged_estate(tmp_path, monkeypatch)
    anchor = sorted(_GRANDFATHERED_UNPINNED_RESTSAMPS)[0]
    monkeypatch.setattr("tests._evidence.evidence_ref", lambda: anchor)
    record = _synthetic_record(
        staged, head=anchor, ts="2026-01-01T00:00:00", run="20260101T000000-1-liverun0"
    )
    _append_record(staged, _SINKS[0], record)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    assert not any("SKIPPING" in str(warning.message) for warning in caught), [
        str(warning.message) for warning in caught
    ]

    (staged / f"redlog-captures/{record['run']}/synthetic-pin.json").unlink()
    with pytest.raises(AssertionError) as failure:
        test_every_law_stamped_redlog_entry_cites_a_live_capture()
    assert "does not EXIST" in str(failure.value), str(failure.value)
