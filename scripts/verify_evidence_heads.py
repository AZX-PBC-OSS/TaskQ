"""THE ARTIFACTS' HEAD-STAMP LAW (the evidence-integrity round, cure 5 —
the receipts law extended to the artifact estate).

THE LAW: **every evidence artifact RECORDS ITS HEAD SHA, and the
verification re-runs the artifact ON ITS CLAIMED HEAD.** The convicted
shape: the "e2e 9/9x3" claim of record pointed at captures that were (a)
RED where the claim said green (leg3 failed in the recorded run) and (b)
taken on an ANCESTOR of the final head (the fix commit landed after
them) — green-in-fact, unproven-as-recorded. A capture without its head
is a rumor; a capture against a stale head is history, never the live
claim.

THE MECHANICS (what this verifier enforces over the estate's registry,
``.measurements/runs/CLAIMS.json``):

* The claims registry is ONE JSON manifest — an append-ordered array of
  ``{stem, file, head_sha, captured_at}`` entries (the writers append:
  ``scripts/check_wf_coverage.py``, ``tests/_wf_fixtures.py``'s band
  writer; the migration pass built the initial index). The verdict is
  THE MANIFEST'S ENTRIES vs HEAD — no filename is ever parsed. (The
  de-slop round's conviction: the verifier's ``_stem``/``_run_order``
  heuristics parsed each filename's run-scoped tail — a NAME-FORMAT law
  the machines enforced, drift-prone by construction: the
  sha-in-the-name one-off stems split wrong. The name is for humans;
  the manifest is for the machinery.)
* Artifacts group by STEM (the evidence kind). The NEWEST entry of a
  stem — the registry's append order — is that evidence kind's LIVE
  CLAIM; every older entry is history, implicitly superseded.
* The live claim must carry ``head_sha`` EQUAL to the current HEAD —
  the verification re-runs the artifact on its claimed head, so the
  claim and the tree can never diverge silently.
* THE OFF-BY-ONE RULE: the artifacts of a battery are committed AFTER
  the battery ran, so a claim recorded on H is still FRESH at H2 when
  H2 is H plus measurement-only changes (nothing outside
  ``.measurements/`` changed since the claimed head — the source tree
  the claim verifies is content-identical). A source change since the
  claimed head makes the claim stale.
* A live claim may be marked ``superseded_by`` (the manifest entry's
  field — the migration bakes in the text files' ``SUPERSEDED-BY:``
  lines): history by its own declaration. THE CHAIN IS THE CLAIM (the
  estate-cures round, cure 1): the verifier resolves the supersession
  CHAIN to its TERMINAL entry — the first entry in the chain with no
  ``superseded_by`` — and applies the head-sha rule THERE: freshness +
  the capture's presence are judged at the END of the chain, not the
  head link (the effective claim is what proves the stem). A dead link
  at ANY hop, a target no registry entry claims (the dangling chain), a
  chain that cycles (it never terminates), and a terminal that is a
  ghost or stale ALL FAIL. The convicted hole this replaces: the old
  law read only "the link must resolve" — the HEAD link's existence was
  the whole check, so a stem GREENED while its effective (terminal)
  claim was stale or missing, the rot hidden one hop behind a resolving
  link (the round-four sin's shape, at the registry's own door).
* A live claim declared ``kind: CITED-IMPORT…`` (the imported
  provenance record — the primary drill's session evidence gone, the
  import IS the provenance) is history by its own declaration: never a
  live claim of the current tree, never re-recordable.
* A registry entry whose capture FILE is missing, and a registry that
  is missing entirely, FAIL — an index naming ghosts is the estate's
  own rot, and an estate without an index is unverifiable.
* A registry that parses and carries ZERO claims is the LOUD REFUSAL,
  never the green (the door-edges round's cure): zero claims is zero
  verdicts — an empty estate CANNOT verify anything, so the empty
  registry exits nonzero with the typed refusal. The WARNING-class
  content (the bootstrapping case is real and honest — a new estate
  has no captures yet) is carried INSIDE the refusal's text; the
  verdict is still never the green.

THE CONTENT DOOR (the content-door round's cure): **the verifier reads
each EFFECTIVE capture's own verdict — a capture whose content records
failure while the claim greens the stem is THE CONTENT-VERDICT
MISMATCH.** The convicted hole this closes: the head-stamp law checked
the stamp's SHA and never the stamp's VERDICT — the live estate carried
captures whose OWN HEADERS say ``rc=1`` (the run failed, in the
capture's first line) standing as the green proof of their stems
(the real instances: ``wf-battery-rv4-20261011T043000Z.txt`` and
``deslop-pin-packs-20261011T042947Z.txt``). The claim's verdict is the
registry's own: a fresh live claim (or a fresh chain terminal) IS the
stem's green — so the door's mismatch test is one-sided by
construction: the content records failure ⇒ the claim lies. The check
is CHEAP — a LINEAR SCAN of the capture's text for VERDICT-SHAPED
lines, never a line-count window, never a structured parse; the
runtime is measured on the real estate (the door-edges round's
receipt — the full-file scan holds the content-door budget). And the
door is HONEST about formats: the families' summary shapes differ (the
pytest tail's ``N failed``, the run-scoped stamp's ``rc=``, the JSON
band payload's measurements), the parser handles the estate's ACTUAL
families — and a capture with NO parsable verdict shape is the TYPED
WARNING, not a silent pass: silence is the defect class this cure
kills. The door judges the claims that GREEN the estate at this head
(the fresh live claim, the fresh chain terminal); a stale or
CITED-IMPORT claim is already judged by its own law, and history's
content is history's.

THE FIVE EDGES (the door-edges round's cures — the content door's own
face, each convicted red-first and pinned
``tests/test_evidence_door_rv6_pins.py``):

* **THE SCAN WINDOW IS VERDICT-SHAPED, NEVER LINE-COUNTED** (D1). The
  content door's first cut read only the first 3 + last 12 lines — a
  red verdict pushed MID-FILE by post-run chatter (the NATURAL case:
  pytest piped through ``tee``, which exits 0 honestly) greened with
  red content on disk. The cure: the capture's FULL text is scanned
  for verdict-shaped lines (``rc=``, ``N failed``, ``FAILED …``) WHEREVER
  they sit; the cheap-law balance holds because a linear scan of a
  text file is measured cheap (the content-door round's +0.1s budget
  is the neighborhood — the receipt is re-measured at this cure).
* **THE JSON BODY'S VERDICT FIELDS ARE READ** (D2). A ``*.json``
  capture carrying ``{"rc": 1, "failed": 3}`` parses as valid JSON —
  the suffix alone greened it by construction. The cure: the JSON
  payloads' verdict fields (``rc`` / ``failed`` / ``errors`` — the
  estate's actual band-payload shapes carry none) are read like any
  stamp; a JSON with a failure-shaped verdict is THE CONVICTION; a
  JSON with only measurements greens as before; a torn body stays the
  typed warning.
* **THE EMPTY REGISTRY IS THE LOUD REFUSAL** (D3). A registry with
  ``claims: []`` greened "the law holds" vacuously — zero claims is
  zero verdicts. The cure: the typed refusal above (the bootstrapping
  case named in the refusal's own text, the exit nonzero, NEVER the
  green). A future-only registry (every capture convicted by the
  capture-time door below) reds for the same reason: nothing honest
  was verified.
* **THE CAPTURE-TIME DOOR — ``captured_at`` IS PARSED** (D4). The
  registry's timestamp was never read: a 2030-dated capture with a
  fresh stamp greened. The cure: ``captured_at`` (and the run-scoped
  filename's embedded stamp, where present) is parsed and judged
  against the reader's clock — a capture dated in the FUTURE beyond
  the tolerance is THE CONVICTION, the fresh ``head_sha`` cannot
  rescue the clock it rides on. THE TOLERANCE, stated: one day
  (``_FUTURE_TOLERANCE``) — the writer's wall clock is recorded
  zoned-less (``record_claim`` writes naive local time), so the
  tolerance must absorb a zone-class skew between the writing and
  reading machines; the future-dating class (years) is far past any
  honest skew a day cannot cover.
* **THE VERIFIED HEAD IS THE REPO'S OWN HEAD** (D5, the severe one).
  ``_head`` trusted ``GITHUB_EVENT_PATH``'s pull-request sha as the
  verified head — an attacker-influenced env made the verifier judge
  against a sha that isn't HEAD (observed: rc=0 with the verifier
  printing "recorded at THIS head (ffffffffffff)" for a sha that
  exists nowhere in the repo). The cure: the verified head is
  ``git rev-parse HEAD`` — ALWAYS; the env face is CROSS-CHECKED,
  never trusted: when the event carries a pull-request head sha, that
  sha must exist in THIS checkout's history (an ancestor of the
  checkout) AND differ from it by nothing outside the measurements
  estate — the freshness law's own teeth (the off-by-one rule)
  applied to the env's claim. A forged or impossible sha — or one
  whose source differs from the checked-out tree — is THE CONVICTION,
  never rc=0. When the env sha passes the teeth it verifies nothing
  on its own: the repo's head stands, and the off-by-one rule already
  judges claims recorded at the env's sha against it. THE CARVE-OUT
  IS RETIRED, named: the old merge-ref escape (verify the PR's own
  head because the merge checkout carries main's side) was the
  forged-head hole itself — the env's say-so installing a head — and
  no workflow in this repo consumes it; under the cured law a CI PR
  checkout greens exactly when its checked-out tree is
  source-identical to the claims' heads, which is the law's own
  honest bar everywhere else.

THE CONTRACT RULING (the unparsable-capture face — stated where the
front flagged it): the front argued a LIVE claim whose verdict cannot
be read should arguably RED, not warn — the split law (the live claim
unparsable = the conviction; the staged/no-verdict declaration = the
warning). THE READS DECIDED AGAINST THE SPLIT: the landed chain-cure's
own control pin (``test_the_honest_chain_stays_green``) builds an
honest, fresh chain terminal whose synthetic capture carries no
verdict shape — the conviction side of the split would red the landed
law's own control, and the estate's honest shapes (the staged slot's
declaration is a convention of CONTENT, not of registry structure) are
not separable from it by anything the verifier can read. The typed
WARNING therefore stands for every unparsable capture — the ruling's
defensible core (silence is the defect) is kept either way: an
unparsable capture is never a silent pass. This paragraph IS the
recorded decision; a future round that can distinguish the staged
declaration structurally (a registry ``kind:`` field for slots) may
revisit.

Usage:  python scripts/verify_evidence_heads.py [--runs-dir .measurements/runs]

Exits 1 with the named stale claims, content-verdict mismatches,
future-dated captures, and the env-forged head's conviction.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

from taskq.testing._claims import RUNS, load_claims

#: THE EVIDENCE BRANCH (the extraction's home — the reviewable PR stops
#: carrying the estate; the record stays durable and public on its own
#: branch). The default resolution order: an explicit --evidence-ref, the
#: local .measurements/ (the dev loop's own shape, when the tree still
#: carries it), then this ref.
DEFAULT_EVIDENCE_REF = "origin/evidence/taskqflow"


def _evidence_ref_resolves(ref: str) -> bool:
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0


def _stage_evidence_ref(ref: str) -> Path:
    """THE EXTRACTED ESTATE'S READ FACE: stage the ref's .measurements/
    estate into a temp dir and return its runs/ — every read path (the
    registry, the captures, the supersession walk, the content door)
    reads the SAME files they always did, from the branch's snapshot.
    The extraction is not a weakening: the claims' head_sha is still
    judged against THIS repo's HEAD (the freshness teeth unchanged)."""
    if not _evidence_ref_resolves(ref):
        fallback = ref.removeprefix("origin/")
        if _evidence_ref_resolves(fallback):
            ref = fallback
        else:
            raise SystemExit(
                f"the evidence ref {ref!r} does not resolve — the estate was "
                "EXTRACTED to the evidence branch (the PR stops carrying it). "
                "Fetch it: git fetch origin evidence/taskqflow (the CI face: "
                "actions/checkout with fetch-depth: 0, or an explicit fetch "
                "of evidence/taskqflow before this script runs)."
            )
    staging = Path(tempfile.gettempdir()) / (
        "evidence-estate-"
        + subprocess.run(
            ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()[:12]
    )
    if not (staging / "runs" / "CLAIMS.json").is_file():
        archive = subprocess.run(
            ["git", "archive", ref, ".measurements"],
            cwd=REPO,
            capture_output=True,
            check=False,
        )
        if archive.returncode != 0:
            raise SystemExit(
                f"the evidence ref {ref!r} carries no .measurements/ estate "
                "(git archive failed) — the extraction's home is wrong"
            )
        import tarfile

        staging.mkdir(parents=True, exist_ok=True)
        import io

        with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
            tar.extractall(staging)  # noqa: S202  # Why: the archive is the repo's own ref's tree, never untrusted input.
    return staging / ".measurements" / "runs"


REPO = Path(__file__).resolve().parent.parent

#: THE VERDICT SHAPES the capture families actually write, scanned
#: across the capture's FULL text (the door-edges round's cure — the
#: first cut's first-3/last-12 window let a red verdict pushed mid-file
#: green silently; a linear scan of a text file is measured cheap, the
#: content-door round's +0.1s budget is the neighborhood). The
#: run-scoped stamp records ``rc=N`` (the writer's own header); pytest's
#: tail records ``N failed``; the short-summary names ``FAILED tests/…``
#: lines even when the count is suppressed. ``0 failed``/``rc=0`` are
#: the GREEN words — only a nonzero count or rc is a failure.
_RC_STAMP = re.compile(r"\brc=(\d+)\b")
_FAILED_COUNT = re.compile(r"\b(\d+) failed\b")
_FAILED_LINE = re.compile(r"^FAILED\s")
#: The GREEN verdict shapes (the parsable families' success words): the
#: stamp's rc, pytest's passed count, pyright's zero-errors. A capture
#: showing NONE of the shapes (failure or green) is UNPARSABLE — the
#: typed warning, never a silent pass.
_GREEN_SHAPES = (
    _RC_STAMP,
    re.compile(r"\b\d+ passed\b"),
    re.compile(r"\b0 errors\b"),
)

#: THE JSON VERDICT KEYS (the door-edges round's cure — the suffix alone
#: used to green a JSON capture by construction). The estate's actual
#: band payloads carry none of these (they record MEASUREMENTS); a
#: payload that DOES carry them gets its verdict read like any stamp.
_JSON_RC_KEYS = ("rc", "returncode")
_JSON_COUNT_KEYS = ("failed", "errors")

#: THE RUN-SCOPED FILENAME'S EMBEDDED STAMP (the second clock seat, read
#: where present — ``record_claim``'s families name their captures with
#: the capture instant: ``…-20261011T200831Z.txt``).
_FILE_STAMP = re.compile(r"\d{8}T\d{6}Z?")

#: THE CAPTURE-TIME DOOR'S TOLERANCE (D4's stated tolerance): one day.
#: The writer's wall clock is recorded ZONED-LESS (``record_claim``
#: writes naive local time), so the tolerance must absorb a zone-class
#: skew between the writing and reading machines — a day covers every
#: honest clock/zone skew; the future-dating class (the convicted
#: 2030-dated capture) is years past it.
_FUTURE_TOLERANCE = timedelta(hours=24)


def _git(*args: str, check: bool = True) -> str:
    out = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=check)
    return out.stdout.strip()


def _verified_head() -> tuple[str, str | None]:
    """The verified head — THE REPO'S OWN HEAD — and the env face's
    conviction, if the env's claim fails the law's cross-check.

    THE LAW (the door-edges round's D5 cure): the head the claims are
    verified against is ``git rev-parse HEAD``, env-influenceable
    never. The ``GITHUB_EVENT_PATH`` face (the CI PR checkout) is
    CROSS-CHECKED, never trusted: when the event payload carries a
    pull-request head sha, that sha must be an ANCESTOR of the
    checked-out HEAD (it must EXIST in this checkout's history) and
    its diff from the checkout must carry NOTHING outside the
    measurements estate — the freshness law's own teeth (the off-by-one
    rule) applied to the env's claim. A forged or impossible sha — or
    one whose source differs from the checked-out tree — is THE
    CONVICTION (returned as the second element), never rc=0. When the
    env sha passes the teeth it verifies nothing on its own: the
    repo's head stands, and the off-by-one rule already judges claims
    recorded at the env's sha against it (the old merge-ref carve-out —
    verify the PR's own head — was the forged-head hole itself: the
    env's say-so installing a head; retired, see the module docstring).
    """
    head = _git("rev-parse", "HEAD")
    event = os.environ.get("GITHUB_EVENT_PATH")
    if not event or not Path(event).is_file():
        return head, None
    try:
        payload: object = json.loads(Path(event).read_text())
    except (json.JSONDecodeError, OSError):
        return head, None  # not a PR event (a push/schedule run) — the checkout IS the head
    pr = (
        cast("dict[str, object]", payload).get("pull_request")
        if isinstance(payload, dict)
        else None
    )
    pr_head = cast("dict[str, object]", pr).get("head") if isinstance(pr, dict) else None
    sha = cast("dict[str, object]", pr_head).get("sha") if isinstance(pr_head, dict) else None
    if not isinstance(sha, str) or not sha or sha == head:
        return head, None
    if _off_by_one_fresh(sha):
        return head, None  # the env's sha is this checkout's history, source-identical — it stands
    return head, (
        f"the GITHUB_EVENT_PATH face names the pull-request head {sha[:12]} — and the "
        f"law's cross-check REFUSES it: the sha is not an ancestor of the checked-out "
        f"HEAD {head[:12]} (it exists nowhere in this checkout's history), or its diff "
        "from the checkout carries source changes. THE ENV-FORGED HEAD is THE "
        "CONVICTION, never rc=0: the verified head is THE REPO'S OWN HEAD — an env's "
        "say-so cannot install one, and a sha the repo cannot name as this checkout's "
        "history never verifies the estate."
    )


def _source_changes_since(head_sha: str) -> bool:
    """Whether ANY commit since *head_sha* touched anything OUTSIDE the
    measurements estate. THE OFF-BY-ONE RULE's diff half (see the module
    docstring): a claim recorded on H is fresh at H2 when H2 is H plus
    measurement-only changes. An unresolvable claimed head (not an
    ancestor) = stale."""
    proc = subprocess.run(
        ["git", "diff", "--name-only", f"{head_sha}..HEAD", "--", ".", ":(exclude).measurements"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return True  # the claimed head is not resolvable — stale by definition
    return bool(proc.stdout.strip())


def _off_by_one_fresh(recorded: str) -> bool:
    """THE OFF-BY-ONE RULE with its own teeth (the door-edges round's
    non-ancestor-stamp cure, taken — the fix is one cheap subprocess):
    *recorded* must be an ANCESTOR of HEAD — a sha that resolves
    nowhere, or a discarded/orphan commit that exists in the object
    store but is no part of this checkout's history, can never verify
    the checked-out tree — AND the diff since it must touch nothing
    outside the measurements estate. ``git merge-base --is-ancestor``
    exits nonzero for both the unresolvable and the non-ancestor sha
    (one subprocess, the walk's cost — measured with the rest)."""
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", recorded, "HEAD"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    if ancestor.returncode != 0:
        return False
    return not _source_changes_since(recorded)


def _supersession_target(marked: str, runs_dir: Path) -> Path:
    """The ``superseded_by`` link's target path (the manifest entry's
    field spells the capture three ways — runs-relative, estate-relative,
    or absolute; the migration's three shapes, honored as recorded)."""
    if marked.startswith(".measurements"):
        return REPO / marked
    if Path(marked).is_absolute():
        return Path(marked)
    if runs_dir.is_absolute():
        return runs_dir.parent / marked
    return REPO / runs_dir.parent / marked


def _json_verdict(payload: object) -> tuple[str, str] | None:
    """The JSON body's OWN verdict, read from its verdict fields (the
    door-edges round's D2 cure — the suffix alone used to green a JSON
    capture by construction).

    Returns ``(status, evidence)`` where status is ``"failure"`` (a
    verdict field carries a failing value: a nonzero ``rc``/
    ``returncode``, a positive ``failed``/``errors`` count), ``"green"``
    (verdict fields present, none failing), or ``None`` — NO verdict
    fields at all (the estate's actual band payloads: the body records
    MEASUREMENTS, and the JSON family's old green stands).
    """
    if not isinstance(payload, dict):
        return None
    body = cast("dict[str, object]", payload)
    fields: list[str] = []
    failure = False
    for key in _JSON_RC_KEYS:
        value = body.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            fields.append(f"{key}={value}")
            failure = failure or value != 0
    for key in _JSON_COUNT_KEYS:
        value = body.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            fields.append(f"{key}={value}")
            failure = failure or value > 0
    if not fields:
        return None
    evidence = "the JSON body's own verdict fields (" + ", ".join(fields) + ")"
    return ("failure" if failure else "green", evidence)


def _content_verdict(path: Path) -> tuple[str, str, int | None]:
    """The capture's OWN verdict, parsed from its content — cheaply.

    THE SCAN IS THE FULL FILE (the door-edges round's D1 cure): every
    line is scanned for the verdict shapes, WHEREVER they sit — the
    first cut's first-3/last-12 window let a red verdict pushed mid-file
    by post-run chatter (pytest piped through ``tee``, which exits 0
    honestly) green with red content on disk. A linear scan of a text
    file is measured cheap (the runtime receipt).

    Returns ``(status, evidence, line)`` where status is:

    * ``"failure"`` — the content records the run failing (a nonzero
      rc, a nonzero failed count, or a FAILED short-summary line,
      ANYWHERE in the capture); *evidence* quotes the capture's own
      failing words at *line* (1-based);
    * ``"green"`` — the content records a verdict and it is not a
      failure (rc=0, the green pytest count, pyright's zero errors, or
      the JSON family's verdict-free measurements);
    * ``"unparsable"`` — NO verdict shape the door parses (a verdict
      shape may be new — TEACH THE DOOR, never teach the capture to be
      silent).
    """
    if path.suffix == ".json":
        # The JSON families: the body is loaded (a torn/unparsable body
        # is the warning) and its VERDICT FIELDS are read — the suffix
        # alone never greens a failure-shaped body (D2).
        try:
            payload: object = json.loads(path.read_text())
        except (ValueError, OSError):
            return ("unparsable", "the JSON its suffix declares does not load", None)
        verdict = _json_verdict(payload)
        if verdict is None:
            return ("green", "the JSON band family (measurements, no verdict fields)", None)
        return (verdict[0], verdict[1], None)
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return ("unparsable", "the capture's content is unreadable", None)
    failure: tuple[str, int] | None = None
    parsable = False
    for lineno, line in enumerate(text.splitlines(), start=1):
        rc = _RC_STAMP.search(line)
        if rc is not None:
            parsable = True
            if int(rc.group(1)) != 0 and failure is None:
                failure = (f"rc={rc.group(1)}", lineno)
        count = _FAILED_COUNT.search(line)
        if count is not None:
            parsable = True
            if int(count.group(1)) > 0 and failure is None:
                failure = (count.group(0), lineno)
        if _FAILED_LINE.match(line) and failure is None:
            failure = ("the FAILED short-summary line", lineno)
        if not parsable and any(shape.search(line) for shape in _GREEN_SHAPES):
            parsable = True
    if failure is not None:
        return ("failure", failure[0], failure[1])
    if parsable:
        return ("green", "the capture's green verdict", None)
    return ("unparsable", "the capture carries no rc=, no passed/failed summary", None)


def _parse_stamp(raw: str) -> datetime | None:
    """The capture instant, parsed — ``None`` when the seat carries no
    parsable stamp (the legacy/synthetic ``t0``-style seats: the door
    judges only what parses, it never guesses)."""
    try:
        stamp = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        # The writer's zoned-less wall clock (record_claim's shape),
        # read in the reader's zone — the tolerance absorbs the skew.
        return stamp.astimezone()
    return stamp


def _capture_time_door(stem: str, entry: dict[str, str], failures: list[str]) -> None:
    """THE CAPTURE-TIME DOOR (the door-edges round's D4 cure): the
    effective claim's capture clocks — the registry's ``captured_at``
    and the run-scoped filename's embedded stamp, where present — are
    PARSED and judged against the reader's clock. A capture dated in
    the FUTURE beyond the tolerance is THE CONVICTION: no honest run
    produces a capture before it runs, and the fresh ``head_sha``
    cannot rescue the clock it rides on. Unparsable seats are never
    guessed at (the door judges only what parses)."""
    now = datetime.now().astimezone()
    seats: list[tuple[str, str, datetime]] = []
    raw_stamp = entry.get("captured_at") or ""
    parsed_stamp = _parse_stamp(raw_stamp) if raw_stamp else None
    if parsed_stamp is not None:
        seats.append(("the registry's captured_at", raw_stamp, parsed_stamp))
    file_stamp = _FILE_STAMP.search(entry.get("file", ""))
    if file_stamp is not None:
        raw_file = file_stamp.group(0)
        parsed_file = _parse_stamp(raw_file[:-1] if raw_file.endswith("Z") else raw_file)
        if parsed_file is not None:
            seats.append(("the filename's embedded stamp", raw_file, parsed_file))
    for seat, raw, stamp in seats:
        ahead = stamp - now
        if ahead <= _FUTURE_TOLERANCE:
            continue
        failures.append(
            f"{stem}: the FUTURE-DATED CAPTURE at {entry['file']} — {seat} reads {raw}, "
            f"which is {ahead} ahead of the reader's clock (the law's tolerance is "
            f"+{_FUTURE_TOLERANCE}, stated in the module docstring). A capture dated in "
            "the future cannot have been produced by any honest run, and the fresh "
            "head_sha cannot rescue the clock it rides on — the future-dating class is "
            "THE CONVICTION, never the green."
        )


def _content_door(
    stem: str,
    entry: dict[str, str],
    runs_dir: Path,
    failures: list[str],
    warnings: list[str],
) -> None:
    """THE CONTENT DOOR at the claim that greens the stem: the
    effective capture's own verdict is parsed and checked AGAINST the
    claim. A capture whose content records failure while the claim
    greens the stem is THE CONTENT-VERDICT MISMATCH (a conviction);
    a capture with no parsable verdict is the TYPED WARNING (never a
    silent pass). Cheap by construction: a linear verdict-shape scan
    over the capture's text, no structured parse (the runtime
    receipt)."""
    base = runs_dir if runs_dir.is_absolute() else REPO / runs_dir
    path = base / entry["file"]
    status, evidence, lineno = _content_verdict(path)
    if status == "green":
        return
    if status == "unparsable":
        warnings.append(
            f"WARNING (unparsable-capture): {stem}: the capture {entry['file']} records "
            f"NO verdict shape the content door parses ({evidence}) — judged on the "
            "head-stamp law alone. If this family's verdict shape is new, TEACH THE "
            "DOOR; never teach the capture to be silent."
        )
        return
    failures.append(
        f"{stem}: the CONTENT-VERDICT MISMATCH at {entry['file']}"
        + (f":{lineno}" if lineno is not None else "")
        + f' — the capture\'s own content records FAILURE ("{evidence}") while the '
        "claim greens the stem: the verifier that never read capture content was "
        "greenstamping live captures whose own words record the run's failure. "
        "Re-run the capture at its head — the honest re-record — or correct the "
        "claim; never leave a red capture standing as the green proof."
    )


def verify(runs_dir: Path, head: str) -> tuple[list[str], list[str]]:
    """The stale live claims and content-verdict mismatches, named (the
    failures empty = the estate holds; the warnings name every capture
    the content door could not parse — surfaced, never silent)."""
    if not (runs_dir / "CLAIMS.json").is_file():
        return (
            [
                f"{runs_dir / 'CLAIMS.json'}: the claims registry is missing — "
                "the estate has no index, so no capture is verifiable (the writers "
                "append one entry per capture; the migration pass built the initial one)"
            ],
            [],
        )
    entries = load_claims(runs_dir)
    if not entries:
        # THE EMPTY REGISTRY IS THE LOUD REFUSAL (the door-edges round's
        # D3 cure): zero claims is zero verdicts — the loop over zero
        # stems proves nothing, and the vacuous "the law holds" was the
        # conviction. The refusal carries the warning-class content (the
        # bootstrapping case is real and honest) INSIDE the nonzero
        # verdict: the state is honest, the verdict is never the green.
        return (
            [
                f"{runs_dir / 'CLAIMS.json'}: the claims registry is EMPTY — zero "
                "claims, zero verdicts: an estate with no claims CANNOT verify "
                "anything, and the law never greens on no evidence (the vacuous pass "
                "is the conviction this refusal kills). WARNING (empty-estate): the "
                "bootstrapping case is real and honest — a new estate has no captures "
                "yet, and this is not corruption (the MISSING registry is that "
                "failure). Record the FIRST claim — any writer's append "
                "(taskq.testing._claims.record_claim) — and the law has something to "
                "judge; until then the empty registry is the loud refusal, never the "
                "green."
            ],
            [],
        )
    # Group by stem; the registry's append order is the run order — the
    # LAST entry of a stem is the live claim. No filename is parsed: the
    # manifest is the index (the name only ever points at the file).
    stems: dict[str, list[dict[str, str]]] = {}
    # The registry entry per capture path (the chain resolver's index):
    # the same file may be claimed by several stems — the registry's
    # append order decides (the LAST claim of a file owns the walk).
    by_path: dict[str, dict[str, str]] = {}
    for entry in entries:
        stem = entry.get("stem", "")
        file = entry.get("file", "")
        if not stem or not file:
            failures_entry = entry.get("file") or entry.get("stem") or json.dumps(entry)
            return (
                [f"the claims registry carries a malformed entry: {failures_entry}"],
                [],
            )
        stems.setdefault(stem, []).append(entry)
        by_path[str((runs_dir / file).resolve())] = entry
    failures: list[str] = []
    warnings: list[str] = []
    for stem, entries in sorted(stems.items()):
        live = entries[-1]
        marked = live.get("superseded_by")
        if marked:
            # THE CHAIN LAW (the module docstring): walk the supersession
            # chain to its TERMINAL entry, then judge the terminal by the
            # head-sha rule. Every hop's content is checked (a chain
            # through a missing capture proves nothing), the walk must
            # terminate, and the termination point must be a real, fresh
            # capture — the effective claim is the chain's END.
            chain: list[dict[str, str]] = []
            current = live
            terminal: dict[str, str] | None = None
            while True:
                link = current.get("superseded_by")
                if not link:
                    terminal = current
                    break
                target = _supersession_target(link, runs_dir)
                if not target.is_file():
                    failures.append(
                        f"{stem}: the supersession chain's link at {current['file']} "
                        f"names {link} but the target does not exist — the chain is "
                        "dead mid-walk (content is checked at EVERY hop, not just the "
                        "head link: a chain through a missing capture proves nothing)"
                    )
                    break
                owner = by_path.get(str(target.resolve()))
                if owner is None:
                    failures.append(
                        f"{stem}: the supersession chain's link at {current['file']} "
                        f"names {link} — the file exists but NO registry entry claims "
                        "it, so the chain dangles past the index (the manifest is the "
                        "estate's only index; a capture outside it does not exist to "
                        "the verdict, and an unowned terminal proves nothing)"
                    )
                    break
                if any(owner is walked for walked in chain):
                    walked_path = " → ".join(w["file"] for w in [*chain, current, owner])
                    failures.append(
                        f"{stem}: the supersession chain CYCLES — the walk "
                        f"{walked_path} re-enters itself and never reaches a "
                        "terminal entry; a chain with no end has no effective "
                        "claim (the self-supersession is the lie here)"
                    )
                    break
                chain.append(current)
                current = owner
            if terminal is None:
                continue
            # THE HEAD-SHA RULE AT THE TERMINAL (the end of the chain):
            # content first (the terminal's capture must EXIST), then
            # freshness — the same rule a live claim carries, applied
            # where the chain actually lands.
            base = runs_dir if runs_dir.is_absolute() else REPO / runs_dir
            if not (base / terminal["file"]).is_file():
                failures.append(
                    f"{stem}: the supersession chain's TERMINAL capture "
                    f"{terminal['file']} is a GHOST — the chain resolves to a "
                    "registry entry whose capture is not on disk (the index and the "
                    "estate disagree at the END of the chain, where the effective "
                    "claim lives; restore the file or extend the chain to a real capture)"
                )
                continue
            recorded = terminal.get("head_sha") or None
            if recorded == head:
                _capture_time_door(stem, terminal, failures)
                _content_door(stem, terminal, runs_dir, failures, warnings)
                continue  # the terminal is FRESH — the chain lands on this head
            if recorded is not None and _off_by_one_fresh(recorded):
                # The off-by-one rule at the terminal (with its ancestry
                # teeth): the terminal's claimed head is an ancestor of
                # HEAD and nothing outside the measurements estate changed
                # since it — the capture verifies a content-identical tree.
                _capture_time_door(stem, terminal, failures)
                _content_door(stem, terminal, runs_dir, failures, warnings)
                continue
            if str(terminal.get("kind", "")).startswith("CITED-IMPORT"):
                # The CITED-IMPORT declaration at the terminal: the chain
                # ends at an imported provenance record — history by its
                # own declaration, never re-recordable (the live claim's
                # own escape, applied where the chain lands).
                continue
            failures.append(
                f"{stem}: the supersession chain's TERMINAL claim {terminal['file']} is "
                + (
                    f"stale — recorded on {recorded[:12]}, the head is {head[:12]}"
                    if recorded
                    else "unstamped — no head_sha recorded"
                )
                + ". The chain's END is the effective claim: re-record it at the "
                "head, or extend the chain to a fresh capture — never leave the "
                "stem's proof stale behind a resolving link."
            )
            continue
        base = runs_dir if runs_dir.is_absolute() else REPO / runs_dir
        missing = base / live["file"]
        if not missing.is_file():
            failures.append(
                f"{stem}: the LIVE CLAIM {live['file']} is a GHOST — the registry "
                "names a capture that is not on disk (the index and the estate "
                "disagree; restore the file or drop the entry)"
            )
            continue
        recorded = live.get("head_sha") or None
        if recorded == head:
            _capture_time_door(stem, live, failures)
            _content_door(stem, live, runs_dir, failures, warnings)
            continue  # the live claim is FRESH — verified on its own head
        if recorded is not None and _off_by_one_fresh(recorded):
            # The off-by-one rule (with its ancestry teeth): the claimed
            # head is an ancestor of HEAD and nothing outside the
            # measurements estate changed since it — the source tree the
            # claim verifies is content-identical. Fresh.
            _capture_time_door(stem, live, failures)
            _content_door(stem, live, runs_dir, failures, warnings)
            continue
        if str(live.get("kind", "")).startswith("CITED-IMPORT"):
            # THE CITED-IMPORT DECLARATION (the docs-numbers round's own
            # marker): the artifact is an IMPORTED provenance record —
            # the primary drill's session evidence is GONE, the imported
            # record IS the provenance (``_sweep.py``'s curve cites it as
            # such). It is not a live claim of THIS tree and cannot be
            # re-recorded; the SOURCE claim built on it is a doc claim
            # whose substance is owned by the scope pin. History by its
            # own declaration.
            continue
        failures.append(
            f"{stem}: the LIVE CLAIM {live['file']} is "
            + (
                f"stale — recorded on {recorded[:12]}, the head is {head[:12]}"
                if recorded
                else "unstamped — no head_sha recorded"
            )
            + ". Re-record it at the head, or mark the entry superseded_by "
            "with the link — never left as the live claim."
        )
    return (failures, warnings)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, default=RUNS)
    parser.add_argument(
        "--evidence-ref",
        default=None,
        help=(
            "the git ref whose .measurements/ estate is the claims' home "
            "(the EXTRACTED estate's face — the evidence lives on "
            "evidence/taskqflow, the reviewable PR stops carrying it): the "
            "ref's registry is read and every claim is judged against THIS "
            "repo's HEAD — the head-stamp law's teeth unchanged (a claim's "
            "head_sha must equal the checked-out HEAD, or be its ancestor "
            "plus measurement-only). Unset, the local .measurements/ is "
            "used when present (the dev loop's own shape)."
        ),
    )
    args = parser.parse_args()
    runs_dir: Path = args.runs_dir
    if args.evidence_ref:
        runs_dir = _stage_evidence_ref(args.evidence_ref)
    elif not (RUNS / "CLAIMS.json").is_file() and _evidence_ref_resolves(DEFAULT_EVIDENCE_REF):
        # THE EXTRACTED ESTATE'S DEFAULT: no local registry (the tree
        # stopped carrying it) — read the evidence branch's estate.
        runs_dir = _stage_evidence_ref(DEFAULT_EVIDENCE_REF)
    head, conviction = _verified_head()
    if conviction is not None:
        # THE ENV-FORGED HEAD (D5): the env's claim failed the
        # cross-check — the conviction, never rc=0.
        print(f"THE HEAD-STAMP LAW REDS (head {head[:12]}):")
        print(f"  {conviction}")
        return 1
    failures, warnings = verify(runs_dir, head)
    if failures:
        print(f"THE HEAD-STAMP LAW REDS (head {head[:12]}):")
        for failure in failures:
            print(f"  {failure}")
        for warning in warnings:
            print(f"  {warning}")
        return 1
    print(
        f"the head-stamp law holds: every evidence kind's live claim in "
        f"{runs_dir} is recorded at THIS head ({head[:12]}) or "
        "explicitly superseded"
    )
    for warning in warnings:
        print(f"  {warning}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
