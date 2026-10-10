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
import subprocess
from hashlib import sha256
from pathlib import Path
from typing import Any

from tests._wf_fixtures import MEASUREMENTS, REDLOG_LAW, head_sha, source_changes_since

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


def _sink_records() -> list[tuple[Path, dict[str, Any]]]:
    """(sink path, record) for every law-stamped record in every sink.
    Pre-law records are HISTORY (the append-only law preserves them);
    they carry no receipt citations and the guard demands none of them —
    the law's records are the live claims."""
    out: list[tuple[Path, dict[str, Any]]] = []
    for name in _SINKS:
        sink = MEASUREMENTS / name
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
    return out


def verify_receipt(record: dict[str, Any], entry: dict[str, Any], measurements: Path) -> str | None:
    """The receipt chain's verdict for ONE entry: ``None`` = the chain
    holds; otherwise the FIRST broken link, named. The three links:

    (a) the entry cites its drill's run-id (the record's own run);
    (b) the entry cites the drill's captured output file path;
    (c) the file EXISTS, its sha256 matches, and its content contains
        the recorded red (the capture's own observed, byte-equal).
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
    capture = measurements / capture_rel
    if not capture.is_file():
        return (
            f"pin={entry.get('pin')!r}: the cited capture {capture_rel} does "
            "not EXIST — the receipt chain's link (c) is broken (a red that "
            "never ran, or a capture deleted since)"
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
    for sink, record in _sink_records():
        claimed = record.get("head_sha")
        live = isinstance(claimed, str) and (
            claimed == _head() or not source_changes_since(claimed)
        )
        if not live:
            history_records += 1
            continue
        live_records += 1
        for entry in record.get("entries", []):
            broken = verify_receipt(record, entry, MEASUREMENTS)
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


def test_the_captures_home_is_not_gitignored() -> None:
    """THE SEVERED-RECEIPT CURE'S STRUCTURAL HALF: the captures' home
    rides WITH the sinks. The convicted shape: ``.measurements/`` was
    gitignored wholesale, so the carries that force-added the sinks
    landed rows whose capture files were invisible residue — dead
    receipts by construction (a THOUSAND of them). The negation pattern
    in :file:`.gitignore` re-includes ``redlog-captures/``; this pin
    holds it: a NEW capture written now must be trackable (git sees
    it), and the ignore's own text names the law."""
    probe = MEASUREMENTS / "redlog-captures" / "._gitignore_probe.json"
    probe.write_text("{}\n", encoding="utf-8")
    try:
        listed = subprocess.run(
            ["git", "status", "--porcelain", "--", str(probe.relative_to(MEASUREMENTS.parent))],
            cwd=MEASUREMENTS.parent,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        assert str(probe.relative_to(MEASUREMENTS.parent)) in listed, (
            "THE REDLOG CAPTURES' HOME IS IGNORED: a new capture under "
            ".measurements/redlog-captures/ is invisible to git — the "
            "sinks will carry rows whose receipts are untracked residue "
            "(the severed-receipt defect again). The .gitignore's "
            "negation pattern must keep the captures' home trackable."
        )
    finally:
        probe.unlink(missing_ok=True)


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
    capture_of = lambda entry: measurements_root / entry["capture"]  # noqa: E731
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
