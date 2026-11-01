"""FV-DELIVERABLE — THE REDLOG LAW GUARD, RE-WRITTEN TO PROVENANCE (the
findings-validation round; the evidence-integrity round's cure 3).

THE LAW (the new evidence law this round enforces): **a redlog entry
CITES ITS RECEIPT CHAIN, or it doesn't exist.**

The guard's first body (the shape rule) AST-walked every test module and
convicted ``.red(...)`` calls whose ``observed`` payload was composed
entirely of syntactic constants — a hand-written literal is prose
wearing an evidence file's clothes. THE SHAPE RULE WAS DEFEATABLE, twice
over, live:

1. **THE KEYWORD-FORM BYPASS** — ``log.red('pin', 'mut', observed={...})``
   has two positional args; the AST scan read ``node.args[2]``, found
   nothing, and SKIPPED INSPECTION. A fabricated payload riding the
   keyword never saw the guard.
2. **THE NAME-LAUNDERED CONSTANT** — ``log.red('pin', 'mut',
   FABRICATED)`` where ``FABRICATED`` is a module-level literal dict: the
   payload node is an ``ast.Name``, not a literal — the scan passed it.
   The lie moved one hop up the module and the guard went blind.

THE CURE IS THE PROVENANCE RULE, NOT A SHAPE RULE: the guard no longer
inspects the CALL at all — it verifies the SINK's RECEIPT CHAIN. Every
entry a law-stamped run recorded must cite (a) its drill's RUN-ID, (b)
the drill's captured output FILE PATH, (c) the capture's sha256 — and
the guard RE-VERIFIES at guard time: the cited file EXISTS, its digest
matches, and its content IS the recorded red. An entry whose cited
capture is missing or mismatching FAILS the guard — whatever syntactic
shape produced it. The two bypasses are re-tested below and both are
convicted BY PROVENANCE: the keyword form and the laundered name now
produce receipts like any other call (the old blind spots are verified
entries), and the conviction of a FABRICATED entry is its broken receipt
— a missing capture, a digest mismatch, a content mismatch — never the
payload's grammar.

(A mechanical guard cannot read minds: a pin that writes the same lie
into its drill capture AND its entry leaves a self-consistent artifact —
which is why the mutation drills' teeth are BEHAVIORAL pins
(``test_wf_sweep_pins.py`` pin 23's law): the lie flips a green test the
moment the mutation ships. The receipt chain's convictions are the
mechanical half: hand-edited sinks, fabricated citations, stale
captures, cross-tree identity confusion.)
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from typing import Any

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

#: The redlog sinks (the fixtures' filenames — every RedLog instance).
_SINKS: tuple[str, ...] = (
    "pin-reds.json",
    "ledger-pin-reds.json",
    "t10-pin-reds.json",
    "t19-pin-reds.json",
    "t06-propagation-reds.json",
    "t20-pin-reds.json",
    "t21-pin-reds.json",
)


def _head() -> str:
    """The current head's sha (the live record's scoping anchor)."""
    return head_sha()


def _sink_records() -> tuple[list[tuple[Path, dict[str, Any]]], str]:
    """(sink path, record) pairs for every law-stamped record in every
    sink, plus the LIVENESS ANCHOR the records' head stamps are judged
    against.

    Pre-law records are HISTORY (the append-only law preserves them);
    they carry no receipt citations and the guard demands none of them —
    the law's records are the live claims.

    The sinks' root resolves local-FIRST, the evidence branch as the
    fallback: the local ``.measurements/`` when it exists (a battery run
    on this tree appends to the LOCAL sinks — they are the live tree's
    own records, anchored at THIS tree's head), else — a fresh clone
    where the extraction left no local estate — the evidence branch's
    checkout staged through
    :func:`tests._evidence.evidence_measurements_dir`, anchored at THE
    ESTATE REF'S OWN HEAD: the records' home tree. A root absent locally
    must not vacate the guard, and the checkout's head is a different
    lineage from the records' — anchoring the estate's records at the
    checkout's head would history every one of them (the scoping teeth
    would refuse a check-out that never ran a battery, which is the
    checkout's honest state, not the guard's failure).
    """
    if MEASUREMENTS.is_dir():
        measurements = MEASUREMENTS
        anchor = _head()
    else:
        from tests._evidence import evidence_measurements_dir, evidence_ref

        measurements = evidence_measurements_dir()
        anchor = evidence_ref()
    out: list[tuple[Path, dict[str, Any]]] = []
    for name in _SINKS:
        sink = measurements / name
        if not sink.exists():
            continue
        for line in sink.read_text().splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # The sinks' PRE-LAW residue (the old write_text format's
                # tail — a JSON array with merge-conflict markers baked
                # in — survives on the first lines of the append-only
                # files): history, never a law record. The law's records
                # are the ones THIS guard verifies.
                continue
            if record.get("law") == REDLOG_LAW:
                out.append((sink, record))
    return out, anchor


def verify_receipt(record: dict[str, Any], entry: dict[str, Any], measurements: Path) -> str | None:
    """The receipt chain's verdict for ONE entry: ``None`` = the chain
    holds; otherwise the FIRST broken link, named. The links:

    (a) the entry cites its drill's run-id (the record's own run);
    (b) the entry cites the drill's captured output file path;
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
            "provenance law's link (b) is missing (a redlog entry without "
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
    """THE PROVENANCE LAW, SCOPED BY THE HEAD-STAMP LAW: every entry a
    law-stamped run recorded ON A LIVE TREE cites its drill's run-id +
    captured output file, the file exists, its digest matches, and its
    content IS the recorded red. A broken chain on a live record = the
    guard FAILS, the entry named.

    THE LIVE/HISTORY SCOPING (the severed-receipt cure, 2026-10-09):
    a record whose ``head_sha`` is the CURRENT head — or whose tree
    differs from the claimed head by MEASUREMENT-ONLY changes (the
    off-by-one rule, the head-stamp verifier's own) — is a LIVE claim:
    its receipt chain is verified in full. A record stamped at a STALE
    head (source moved past it) is HISTORY — the head-stamp law's own
    verdict ("a capture against a stale head is history, never the
    live claim") — never deleted (the sinks are append-only, zero rows
    dropped), never demanded a file the tree no longer carries. The
    convicted shape that forced the scoping: the batteries' drill runs
    append their rows to the TRACKED sinks while the captures land
    under the (then-)gitignored captures' home — a carry that shipped
    the sinks without the captures left a THOUSAND rows whose receipts
    died with the ephemeral lanes' worktrees. The rows are history;
    the LAW's teeth are unchanged: every live record's chain is fully
    re-verified here, and the drills' convictions re-derive on every
    battery (the behavioral pins — a mutation's red whose cure shipped
    flips a green test, the receipt's real half)."""
    violations: list[str] = []
    live_records = 0
    history_records = 0
    records, anchor = _sink_records()
    for sink, record in records:
        # The receipts resolve against the SAME root the sink was read
        # from (the capture paths are relative to the measurements root;
        # the evidence-branch fallback stages its own copy).
        measurements = sink.parent
        claimed = record.get("head_sha")
        live = isinstance(claimed, str) and (
            claimed == anchor or not source_changes_since(claimed, anchor=anchor)
        )
        if not live:
            history_records += 1
            continue
        live_records += 1
        for entry in record.get("entries", []):
            broken = verify_receipt(record, entry, measurements)
            if broken is not None:
                violations.append(f"{sink.name}: {broken}")
    assert not violations, (
        "THE REDLOG PROVENANCE LAW: a redlog entry cites its receipt chain "
        "(run-id + capture file + digest) or it doesn't exist — these "
        "entries' chains are broken:\n" + "\n".join(violations)
    )
    # THE SCOPING'S OWN TEETH: the guard never runs against an estate
    # with zero live records and a full history — a guard that verifies
    # nothing has no verdict. Some law record must be live at any guard
    # time (the newest battery's rows stamp the current head; the
    # off-by-one keeps them live across the sink's own landing commit).
    assert live_records > 0 or history_records == 0, (
        f"THE REDLOG PROVENANCE LAW'S SCOPING: {history_records} history "
        "records and ZERO live records — the guard verified nothing. A "
        "battery must have run at this head (or a measurement-only delta "
        "of it) before the guard's verdict means anything."
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


def test_the_guard_convicts_the_two_shape_bypasses_by_provenance(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """The shape rule's TWO LIVE BYPASSES, re-tested against the
    provenance rule: NEITHER form escapes the receipt chain.

    * the KEYWORD-FORM payload (the old scan read ``args[2]`` and
      skipped) — records like any call now, its entry VERIFIED by
      receipt; break the receipt (delete the capture) and the guard
      convicts the entry, whatever syntax produced it;
    * the NAME-LAUNDERED CONSTANT (the old scan saw an ``ast.Name`` and
      passed) — same: the entry is verified by its CAPTURE's content,
      not by resolving the name's grammar; mutate the capture behind the
      digest and the guard convicts (the content check).
    """
    from tests._wf_fixtures import RedLog

    sink = tmp_path / "bypass-reds.json"
    monkeypatch.setattr("tests._wf_fixtures.MEASUREMENTS", tmp_path)
    # The captures' root is pinned to the test's own directory: both the
    # sink file and the cited captures resolve inside tmp_path.
    monkeypatch.setenv(REDLOG_CAPTURES_DIR_ENV, str(tmp_path / "captures"))
    measurements_root = tmp_path  # the RedLog module-global is patched above

    log = RedLog(sink.name)

    # BYPASS 1, the keyword form — the old guard's blind spot:
    log.red("bypass-keyword-form", "the payload rides observed=", observed={"zombie_wake": True})
    # BYPASS 2, the name-laundered constant — the old guard saw a Name:
    fabricated_constant = {"zombie_wake": True, "note": "never drilled"}
    log.red("bypass-laundered-constant", "the payload is a module constant", fabricated_constant)
    log.flush()

    records = [json.loads(line) for line in sink.read_text().splitlines()]
    assert len(records) == 1 and records[0]["law"] == REDLOG_LAW
    for entry in records[0]["entries"]:
        # THE PROVENANCE VERDICT: both entries' receipts HOLD (each cites
        # a live, digest-matching, content-matching capture) — the
        # conviction is by the chain, never the shape.
        assert verify_receipt(records[0], entry, measurements_root) is None, entry

    # THE CONVICTIONS (both by provenance):
    keyword_entry, laundered_entry = records[0]["entries"]
    # 1. The keyword-form entry whose capture VANISHED: convicted.
    capture_of = lambda entry: resolve_redlog_capture(  # noqa: E731
        measurements_root, entry["capture"], records[0].get("captures_root")
    )[0]
    capture_of(keyword_entry).unlink()
    broken = verify_receipt(records[0], keyword_entry, measurements_root)
    assert broken is not None and "does not EXIST" in broken, broken
    # 2. The laundered-constant entry whose capture MISMATCHES its
    #    recorded red (the capture rewritten behind the digest — the
    #    stale/mutated receipt): convicted.
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


def test_a_fabricated_sink_entry_without_a_citation_is_convicted(tmp_path: Path) -> None:
    """THE HAND-EDITED SINK (the fabrication vector the shape rule could
    never see — it lived outside the source): an entry appended to a
    sink WITHOUT its receipt chain is convicted at guard time. The red
    that was never run doesn't exist."""
    fabricated = {
        "run": "19700101T000000-0-deadbeef",
        "ts": "1970-01-01T00:00:00",
        "law": REDLOG_LAW,
        "entries": [
            {
                "pin": "fabricated-without-a-drill",
                "mutation": "nothing was mutated",
                "red": '{"zombie_wake": true}',
            }
        ],
    }
    record = {"run": fabricated["run"], "law": REDLOG_LAW, "entries": fabricated["entries"]}
    broken = verify_receipt(record, record["entries"][0], tmp_path)
    assert broken is not None and "cites NO capture" in broken, broken


def test_the_captures_default_outside_the_repo_tree(tmp_path: Path, monkeypatch: Any) -> None:
    """With no pin set, a drill's capture file is written under the
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


def test_a_capture_written_by_another_run_is_convicted(tmp_path: Path) -> None:
    """A cited capture whose OWN payload names another run does not
    prove this run's red — the identity link is checked from the
    capture's content, not the citation's say-so."""
    record, entry = _forged_entry(
        tmp_path, tmp_path / "caps", "pin-x", "20260101T000000-2-beefbeef"
    )
    broken = verify_receipt(record, entry, tmp_path)
    assert broken is not None and "another run" in broken, broken


def test_a_capture_written_for_another_pin_is_convicted(tmp_path: Path) -> None:
    """A cited capture whose OWN payload names another pin does not
    prove this pin's red — the capture must self-identify with the
    entry's pin."""
    record, entry = _forged_entry(
        tmp_path, tmp_path / "caps", "pin-y", "20260101T000000-1-cafecafe"
    )
    broken = verify_receipt(record, entry, tmp_path)
    assert broken is not None and "another pin" in broken, broken


def test_an_escaping_citation_is_refused(tmp_path: Path) -> None:
    """A citation that is absolute or walks out with ``..`` names a file
    outside the estate — refused as its OWN entry's violation (a receipt
    line naming the citation and the face), not an erroring run."""
    record, entry = _forged_entry(tmp_path, tmp_path / "caps", "pin-x", _FORGED_RUN)
    for forged in ("/etc/hostname", "redlog-captures/../../outside.json"):
        escaped = dict(entry, capture=forged)
        broken = verify_receipt(record, escaped, tmp_path)
        assert broken is not None, broken
        assert "REFUSED" in broken and "estate-relative" in broken, broken


def test_a_record_root_inside_the_repo_is_refused(tmp_path: Path) -> None:
    """A record whose own captures_root names a path inside the
    repository containing the record cannot clear its receipt with a
    capture the tree ships itself: that root is refused, and the
    verdict is the missing-capture refusal naming it."""
    capture_root = MEASUREMENTS / "forge-caps"
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
        shutil.rmtree(capture_root, ignore_errors=True)


def test_a_cwd_relative_record_root_inside_the_repo_is_refused(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Same refusal for a record-declared root given as a relative path:
    resolved against the reader's working directory it lands inside the
    repository containing the record, and the resolution skips it."""
    monkeypatch.chdir(MEASUREMENTS.parent)
    capture_root = MEASUREMENTS / "forge-caps"
    record, entry = _forged_entry(tmp_path, capture_root, "pin-x", _FORGED_RUN)
    record["captures_root"] = ".measurements/forge-caps"
    try:
        broken = verify_receipt(record, entry, MEASUREMENTS)
        assert broken is not None, (
            f"the forged receipt passed: the record's relative captures_root "
            f"{record['captures_root']!r} resolves inside the repository "
            "containing the record"
        )
        assert "REFUSED" in broken and str(capture_root) in broken, broken
    finally:
        shutil.rmtree(capture_root, ignore_errors=True)


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
    capture_root = MEASUREMENTS / "forge-caps"
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
        shutil.rmtree(capture_root, ignore_errors=True)


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
