"""The artifact estate's head-stamp verifier.

The rule: **every evidence artifact records its head sha, and the
verification re-runs the artifact on its claimed head.** A capture
without its head is a rumor; a capture against a stale head is history,
never the live claim — a green claim whose captures are stale or
failing where they claim green is unproven.

The mechanics (enforced over the estate's registry,
``.measurements/runs/CLAIMS.json``):

* The claims registry is one JSON manifest — an append-ordered array of
  ``{stem, file, head_sha, captured_at}`` entries (the writers append:
  ``scripts/check_wf_coverage.py``, ``tests/_wf_fixtures.py``'s band
  writer). The verdict is the MANIFEST'S ENTRIES vs HEAD — no filename
  is ever parsed. The name is for humans; the manifest is for the
  machinery (parsing each filename's run-scoped tail would enforce a
  name-format rule on the humans' own files, and the sha-in-the-name
  one-off stems split wrong under it).
* Artifacts group by STEM (the evidence kind). The NEWEST entry of a
  stem — the registry's append order — is that evidence kind's LIVE
  CLAIM; every older entry is history, implicitly superseded.
* The live claim must carry ``head_sha`` EQUAL to the current HEAD —
  the verification re-runs the artifact on its claimed head, so the
  claim and the tree can never diverge silently.
* The off-by-one rule: the artifacts of a battery are committed after
  the battery ran, so a claim recorded on H is still FRESH at H2 when
  H2 is H plus changes to paths that CANNOT change what the claims
  verify — the measurements estate itself plus the NON-BEHAVIORAL paths
  (``_NONBEHAVIORAL_EXCLUSIONS``: docs, the readme/license/metadata
  class, ``.github``) — the source + test tree the claim verifies is
  content-identical. A change to a BEHAVIORAL path (``src/``,
  ``tests/``, the lockfiles, the migrations, the examples, the gates,
  the benchmarks) since the claimed head makes the claim stale.
  The docs-derived stems (``_DOCS_SENSITIVE_STEM_PREFIXES``:
  mkdocs-strict, docs-examples, docs-lane) are the exception — their
  batteries READ the docs estate (mkdocs builds ``docs/`` strict; the
  docs-example tests' ``DOC_ROOTS = ("docs", "README.md")`` execute the
  fences) — so for them the exemption set is the NARROW one (the
  measurements estate + ``.github``) and a docs-only merge stalifies.
  A docs-only merge does not stale a source claim, and the docs-derived
  stems are exempt from that exemption.
* A live claim may be marked ``superseded_by`` (the manifest entry's
  field): history by its own declaration. The chain IS the claim: the
  verifier resolves the supersession chain to its TERMINAL entry — the
  first entry with no ``superseded_by`` — and applies the head-sha rule
  THERE (the effective claim is what proves the stem). A dead link at
  any hop, a target no registry entry claims, a chain that cycles, and
  a terminal that is a ghost or stale ALL FAIL — checking only the
  head link's existence would let a stem green while its effective
  claim was stale or missing.
* A live claim declared ``kind: CITED-IMPORT…`` is history by its own
  declaration: never a live claim of the current tree, never
  re-recordable.
* A registry entry whose capture FILE is missing, and a registry that
  is missing entirely, FAIL — an index naming ghosts is the estate's
  own rot, and an estate without an index is unverifiable.
* A registry that parses and carries ZERO claims is the LOUD REFUSAL,
  never the green: zero claims is zero verdicts — an empty estate
  CANNOT verify anything, so the empty registry exits nonzero with the
  typed refusal. The bootstrapping case (a new estate has no captures
  yet) is carried inside the refusal's text; the verdict is still
  never the green.

The content door: **the verifier reads
each EFFECTIVE capture's own verdict — a capture whose content records
failure while the claim greens the stem is the CONTENT-VERDICT
MISMATCH.** A stamp checked for SHA alone says nothing about verdict;
the claim's verdict is the registry's own: a fresh live claim (or a
fresh chain terminal) IS the stem's green, so the mismatch test is
one-sided by construction: the content records failure ⇒ the claim
lies. The check is CHEAP — a LINEAR SCAN of the capture's text for
VERDICT-SHAPED lines, never a line-count window, never a structured
parse; the full-file scan is measured cheap on the real estate (within
the content-door budget). The families' summary shapes differ (the
pytest tail's ``N failed``, the run-scoped stamp's ``rc=``, the JSON
band payload's measurements); the parser handles the estate's ACTUAL
families — and a capture with no parsable verdict shape is the TYPED
WARNING, not a silent pass: silence is the defect class the door
kills. The door judges the claims that GREEN the estate at this head
(the fresh live claim, the fresh chain terminal); a stale or
CITED-IMPORT claim is already judged by its own law, and history's
content is history's.

The content door's five edges (each pinned
``tests/test_evidence_door_contract_pins.py``):

* **the SCAN WINDOW IS VERDICT-SHAPED, never LINE-COUNTED.** Reading
  only the first 3 + last 12 lines greens a red verdict pushed MID-FILE
  by post-run chatter (the NATURAL case: pytest piped through ``tee``,
  which exits 0 honestly). The capture's FULL text is scanned for
  verdict-shaped lines (``rc=``, ``N failed``, ``FAILED …``) WHEREVER
  they sit; the linear scan of a text file is measured cheap (the
  content-door budget's neighborhood: +0.1s).
* **the JSON BODY'S VERDICT FIELDS ARE READ.** A ``*.json`` capture
  carrying ``{"rc": 1, "failed": 3}`` parses as valid JSON — a suffix
  check alone would green it by construction. The JSON payloads'
  verdict fields (``rc`` / ``failed`` / ``errors``) are read like any
  stamp; a JSON with a failure-shaped verdict is the FAILURE VERDICT; a
  JSON with only measurements greens as before; a torn body stays the
  typed warning.
* **the EMPTY REGISTRY IS the LOUD REFUSAL.** ``claims: []`` greens
  nothing: zero claims is zero verdicts (the typed refusal above, the
  bootstrapping case named in the refusal's own text, the exit
  nonzero, never the green). A future-only registry (every capture
  rejected by the capture-time door below) reds for the same reason:
  nothing honest was verified.
* **the CAPTURE-TIME DOOR — ``captured_at`` IS PARSED.** A 2030-dated
  capture with a fresh stamp does not green. ``captured_at`` (and the
  run-scoped filename's embedded stamp, where present) is parsed and
  judged against the reader's clock — a capture dated in the FUTURE
  beyond the tolerance is the REFUSAL; the fresh ``head_sha`` cannot
  rescue the clock it rides on. the TOLERANCE, stated: one day
  (``_FUTURE_TOLERANCE``) — the writer's wall clock is recorded
  zoned-less (``record_claim`` writes naive local time), so the
  tolerance absorbs a zone-class skew between the writing and
  reading machines; the future-dating class (years) is far past any
  honest skew a day cannot cover.
* **the VERIFIED HEAD IS the REPO'S own HEAD.** ``git rev-parse HEAD``
  is the verified head — always; the ``GITHUB_EVENT_PATH`` face is
  CROSS-CHECKED, never trusted: when the event carries a pull-request
  head sha, that sha must exist in THIS checkout's history (an
  ancestor of the checkout) and differ from it by nothing behavioral —
  the off-by-one rule's own exclusion set applied to the env's claim.
  A forged or impossible sha — or one whose source differs from the
  checked-out tree — is the REFUSAL, never rc=0. When the env sha
  passes the teeth it verifies nothing on its own: the repo's head
  stands, and the off-by-one rule already judges claims recorded at
  the env's sha against it. There is no merge-ref escape: verifying
  the PR's own head because the merge checkout carries main's side
  would BE the forged-head hole (the env's say-so installing a head),
  and no workflow in this repo consumes it; a CI PR checkout greens
  exactly when its checked-out tree is source-identical to the claims'
  heads.

The unparsable-capture ruling: an unparsable live claim stays the
typed WARNING (never a refusal) — an honest, fresh chain terminal
whose synthetic capture carries no verdict shape is a green estate's
own control, so refusing would red the landed law's own control, and
the staged slot's declaration is a convention of CONTENT, not of
registry structure, not separable from it by anything the verifier can
read. The ruling's defensible core holds either way: an unparsable
capture is never a silent pass. A future round that can distinguish
the staged declaration structurally (a registry ``kind:`` field for
slots) may revisit.

Usage:  python scripts/verify_evidence_heads.py [--runs-dir .measurements/runs]

Exits 1 with the named stale claims, content-verdict mismatches,
future-dated captures, and the env-forged head's refusal.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import IO, cast

from taskq.testing._claims import RUNS, load_claims

#: the evidence branch . The default resolution order: an explicit
#: --evidence-ref, the local .measurements/ (the dev loop's own shape, when the
#: tree still carries it), then this ref.
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


#: The staging directories' name prefix, shared by every run and by the
#: startup sweep that cleans up after crashed ones.
_STAGING_PREFIX = "evidence-estate-"

#: The marker + lock file inside every staging directory: its existence
#: scopes the sweep to directories this tooling wrote (a same-named
#: stranger is never touched), and the flock a live reader holds on it
#: keeps the sweep from deleting a staging mid-read.
_STAGING_LOCK_NAME = ".evidence-estate.lock"

#: A staging directory older than this has no live reader: a
#: verification run lasts seconds, so anything older was left by a run
#: that died before it could remove its own staging.
_STAGING_ORPHAN_AGE = timedelta(hours=1)


def _prune_stale_staging(temp: Path | None = None) -> list[str]:
    """Delete staging directories left behind by dead runs, and return
    the pruned names.

    Each run stages into its own ``evidence-estate-*`` directory,
    holds an flock on its ``.evidence-estate.lock`` for its lifetime,
    and removes the directory on exit; a dead process holds nothing.
    A directory under *temp* (default: the system temp dir) is swept
    when it carries the lock file (the marker — directories without it
    were not written by this tooling and are never touched), when its
    lock can be taken (a live reader's flock refuses the attempt), and
    when it is older than the age bar. The lock is checked before the
    age, so a live staging is never a candidate.
    """
    if temp is None:
        temp = Path(tempfile.gettempdir())
    pruned: list[str] = []
    now = time.time()
    for orphan in sorted(temp.glob(f"{_STAGING_PREFIX}*")):
        try:
            lock_path = orphan / _STAGING_LOCK_NAME
            if not orphan.is_dir() or not lock_path.is_file():
                continue  # not this tooling's staging — the marker scopes the glob
            with lock_path.open("a") as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    continue  # a live reader holds it — not an orphan
                try:
                    if now - orphan.stat().st_mtime <= _STAGING_ORPHAN_AGE.total_seconds():
                        continue
                    shutil.rmtree(orphan, ignore_errors=True)
                    pruned.append(orphan.name)
                finally:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        except OSError:
            continue  # a concurrently removed or unreadable entry is not this run's problem
    return pruned


def _stage_evidence_ref(ref: str) -> tuple[Path, Path, IO[str]]:
    """Stage the ref's ``.measurements/`` estate into a fresh temp
    directory and return ``(runs_dir, staging_dir, lock)``. Every read
    path (the registry, the captures, the supersession walk, the
    content door) reads the same files it always did, from the branch's
    snapshot.

    The staging directory is per-invocation scratch: the caller removes
    it once the reads are done — ``main`` does this in a ``finally`` —
    because the archive expands to hundreds of megabytes and a staging
    copy that outlived its run filled the temp filesystem. The lock
    file is flocked for the caller's lifetime, so a concurrent
    startup's prune skips the directory while it is being read; a
    failure at any point after the directory exists removes it before
    propagating. The claims' ``head_sha`` is still judged against THIS
    repo's HEAD (the freshness teeth unchanged)."""
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
    import io
    import tarfile

    staging = Path(tempfile.mkdtemp(prefix=_STAGING_PREFIX))
    lock = (staging / _STAGING_LOCK_NAME).open("a")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
            # The filter also retires the extractall deprecation (3.14's
            # default extraction behavior change).
            tar.extractall(
                staging, filter="data"
            )  # Why: the archive is the repo's own ref's tree, never untrusted input.
    except BaseException:
        lock.close()
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return staging / ".measurements" / "runs", staging, lock


REPO = Path(__file__).resolve().parent.parent

#: the verdict shapes the capture families actually write, scanned
#: across the capture's full text (a first-3/last-12 window greens a red
#: verdict pushed mid-file; a linear scan of a text file is measured
#: cheap — the content-door budget's neighborhood, +0.1s). The
#: run-scoped stamp records ``rc=N`` (the writer's own header); pytest's
#: tail records ``N failed``; the short-summary names ``failed tests/…``
#: lines even when the count is suppressed. ``0 failed``/``rc=0`` are
#: the green words — only a nonzero count or rc is a failure.
_RC_STAMP = re.compile(r"\brc=(\d+)\b")
_FAILED_COUNT = re.compile(r"\b(\d+) failed\b")
_FAILED_LINE = re.compile(r"^FAILED\s")
#: The green verdict shapes (the parsable families' success words): the
#: stamp's rc, pytest's passed count, pyright's zero-errors. A capture
#: showing none of the shapes (failure or green) is unparsable — the
#: typed warning, never a silent pass.
_GREEN_SHAPES = (
    _RC_STAMP,
    re.compile(r"\b\d+ passed\b"),
    re.compile(r"\b0 errors\b"),
)

#: the JSON verdict keys (the suffix alone would green a JSON capture by
#: construction). The estate's actual
#: band payloads carry none of these (they record measurements); a
#: payload that does carry them gets its verdict read like any stamp.
_JSON_RC_KEYS = ("rc", "returncode")
_JSON_COUNT_KEYS = ("failed", "errors")

#: the run-scoped filename's embedded stamp (the second clock seat, read
#: where present — ``record_claim``'s families name their captures with
#: the capture instant: ``…-20261011T200831Z.txt``).
_FILE_STAMP = re.compile(r"\d{8}T\d{6}Z?")

#: one day. The writer's wall clock is recorded zoned-less (``record_claim``
#: writes naive local time), so the tolerance must absorb a zone-class skew
#: between the writing and reading machines — a day covers every honest
#: clock/zone skew; the future-dating class (a capture dated 2030 is rejected
#: as impossible) is years past it.
_FUTURE_TOLERANCE = timedelta(hours=24)

#: the pathspecs the off-by-one rule's diff half ignores for every stem — the
#: measurements estate and the workflow estate (.github). This is the
#: docs-derived stems' set (see ``_DOCS_SENSITIVE_STEM_PREFIXES``): for the
#: evidence kinds whose batteries read the docs estate, a docs-only merge is a
#: source change.
_NARROW_EXCLUSIONS = (
    ":(exclude).measurements",
    ":(exclude).github",
)

#: the evidence kinds whose batteries read the docs estate — mkdocs builds
#: ``docs/`` strict; the docs-example tests
#: (``tests/_docs_examples.py::DOC_ROOTS = ("docs", "readme.md")``) execute the
#: fences those documents carry; the docs-lane captures run the same lanes. For
#: a stem carrying one of these prefixes the docs exemption does not apply (its
#: exclusion set is ``_NARROW_EXCLUSIONS``): ``docs/`` and ``readme.md`` are
#: inputs the capture's verdict was measured against, so a docs-only merge
#: stalifies the claim exactly as a src/ merge would.
_DOCS_SENSITIVE_STEM_PREFIXES = ("mkdocs-strict", "docs-examples", "docs-lane")

#: the pathspecs the off-by-one rule's diff half ignores for every other stem —
#: the narrow set above plus the non-behavioral paths. A plain stem's claim
#: verifies the source + test tree, so the exemption set is exactly the paths
#: that cannot alter that tree — docs and the readme/license/ metadata class —
#: and never the behavioral paths (``src/``, ``tests/``, ``pyproject.toml``,
#: ``uv.lock``, the migrations, the examples the smoke tests import, the
#: scripts the gates run, the benchmarks). the exemption's own edge: the
#: docs-derived stems (``_DOCS_SENSITIVE_STEM_PREFIXES``) are the exception —
#: their batteries read the docs estate — so for them the exemption set is the
#: narrow one and a docs-only merge stalifies (``_source_changes_since`` picks
#: the set per stem). Pathspec shapes (git's anchoring, verified against git
#: 2.56 and pinned by ``tests/test_evidence_docs_exemption_pins.py``): a
#: directory pathspec covers its subtree from the root; ``readme*`` and the
#: ``glob``-magic ``*.md`` match at the root only (glob magic: ``*`` does not
#: cross ``/``) — a nested ``tests/*.md`` capture or a ``src/readme`` still
#: stalifies. the law's second face: the in-suite mirror
#: (``tests/_wf_fixtures.OFF_BY_ONE_DIFF_EXCLUSIONS``) carries the same sets —
#: the sync pin holds the two faces in step.
_NONBEHAVIORAL_EXCLUSIONS = (
    *_NARROW_EXCLUSIONS,
    ":(exclude)LEDGER.md",
    ":(exclude)docs",
    ":(exclude)README*",
    ":(exclude,glob)*.md",
    ":(exclude)LICENSE",
    ":(exclude).gitignore",
)


def _git(*args: str, check: bool = True) -> str:
    out = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=check)
    return out.stdout.strip()


def _verified_head() -> tuple[str, str | None]:
    """The verified head — the REPO'S own HEAD — and the env face's
    refusal, if the env's claim fails the law's cross-check.

    The head the claims are
    verified against is ``git rev-parse HEAD``, env-influenceable
    never. The ``GITHUB_EVENT_PATH`` face (the CI PR checkout) is
    CROSS-CHECKED, never trusted: when the event payload carries a
    pull-request head sha, that sha must be an ANCESTOR of the
    checked-out HEAD (it must EXIST in this checkout's history) and
    its diff from the checkout must carry NOTHING behavioral — the
    off-by-one rule's own exclusion set (the measurements estate plus
    the non-behavioral paths), the freshness law's own teeth (the
    off-by-one rule) applied to the env's claim. A forged or
    impossible sha — or
    one whose source differs from the checked-out tree — is the
    REFUSAL (returned as the second element), never rc=0. When the
    env sha passes the teeth it verifies nothing on its own: the
    repo's head stands, and the off-by-one rule already judges claims
    recorded at the env's sha against it (a merge-ref carve-out —
    verifying the PR's own head — would be the forged-head hole itself:
    the env's say-so installing a head; see the module docstring).
    """
    head = _git("rev-parse", "HEAD")
    event = os.environ.get("GITHUB_EVENT_PATH")
    if not event or not Path(event).is_file():
        return head, None
    try:
        payload: object = json.loads(Path(event).read_text())
    except (json.JSONDecodeError, OSError):
        return head, None  # not a pr event (a push/schedule run) — the checkout is the head
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
        "REFUSAL, never rc=0: the verified head is THE REPO'S OWN HEAD — an env's "
        "say-so cannot install one, and a sha the repo cannot name as this checkout's "
        "history never verifies the estate."
    )


def _source_changes_since(head_sha: str, stem: str | None = None) -> bool:
    """Whether any commit since *head_sha* touched a BEHAVIORAL path.
    For a plain stem that is anything outside the measurements estate
    and the non-behavioral paths (``_NONBEHAVIORAL_EXCLUSIONS``). the
    off-BY-one RULE's diff half (see the module docstring): a claim
    recorded on H is fresh at H2 when H2 is H plus
    measurement-and-docs-only changes — the source + test tree the
    claim verifies is content-identical. the DOCS-DERIVED STEMS'
    FALLBACK: *stem* carrying a
    ``_DOCS_SENSITIVE_STEM_PREFIXES`` prefix names an evidence kind
    whose battery READS the docs estate — for those the exemption set
    is the NARROW one (``_NARROW_EXCLUSIONS``) and a docs-only merge
    stalifies exactly as a src/ merge would. An unresolvable claimed
    head (not an ancestor) = stale."""
    if stem is not None and stem.startswith(_DOCS_SENSITIVE_STEM_PREFIXES):
        exclusions = _NARROW_EXCLUSIONS
    else:
        exclusions = _NONBEHAVIORAL_EXCLUSIONS
    proc = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            f"{head_sha}..HEAD",
            "--",
            ".",
            *exclusions,
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return True  # the claimed head is not resolvable — stale by definition
    return bool(proc.stdout.strip())


def _off_by_one_fresh(recorded: str, stem: str | None = None) -> bool:
    """Off-BY-one RULE with its own teeth
    *recorded* must be an ANCESTOR of HEAD — a sha that resolves
    nowhere, or a discarded/orphan commit that exists in the object
    store but is no part of this checkout's history, can never verify
    the checked-out tree — and the diff since it must touch no
    BEHAVIORAL path (the diff half's exclusion set — the measurements
    estate plus the non-behavioral paths, NARROWED to the measurements
    estate + .github for the DOCS-DERIVED stems, see
    ``_DOCS_SENSITIVE_STEM_PREFIXES``). One cheap subprocess:
    ``git merge-base --is-ancestor``
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
    return not _source_changes_since(recorded, stem)


def _entry_capture_path(base: Path, stem: str, file: str) -> Path:
    """An entry's capture path, resolved under *base* — refused unless
    it stays there. An absolute path, an empty one, or one carrying
    ``..`` names whatever the reader's machine happens to have at the
    escaped location, not a capture of the estate being verified; the
    refusal names the entry. The resolved-path containment is checked
    even for a well-formed relative path, so a symlink planted inside
    the estate cannot walk the read out either."""
    if not file or Path(file).is_absolute() or ".." in Path(file).parts:
        raise SystemExit(
            f"the registry entry for {stem!r} names the capture {file!r} — "
            "refused: a capture path is not an estate-relative path (an "
            "absolute path or one carrying '..' points outside the estate "
            "the verdict reads)"
        )
    candidate = (base / file).resolve()
    if not candidate.is_relative_to(base.resolve()):
        raise SystemExit(
            f"the registry entry for {stem!r} names the capture {file!r} — "
            f"refused: it resolves to {candidate}, outside the estate the "
            "verdict reads"
        )
    return candidate


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
    """The JSON body's own verdict, read from its verdict fields (the
    suffix alone would green a JSON capture by construction).

    Returns ``(status, evidence)`` where status is ``"failure"`` (a
    verdict field carries a failing value: a nonzero ``rc``/
    ``returncode``, a positive ``failed``/``errors`` count), ``"green"``
    (verdict fields present, none failing), or ``None`` — no verdict
    fields at all (the estate's actual band payloads: the body records
    MEASUREMENTS, and the JSON family's green stands).
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
    """The capture's own verdict, parsed from its content — cheaply.

    Every
    line is scanned for the verdict shapes, WHEREVER they sit — a
    first-3/last-12 window would green a red verdict pushed mid-file
    by post-run chatter (pytest piped through ``tee``, which exits 0
    honestly). A linear scan of a text
    file is measured cheap (measured).

    Returns ``(status, evidence, line)`` where status is:

    * ``"failure"`` — the content records the run failing (a nonzero
      rc, a nonzero failed count, or a FAILED short-summary line,
      ANYWHERE in the capture); *evidence* quotes the capture's own
      failing words at *line* (1-based);
    * ``"green"`` — the content records a verdict and it is not a
      failure (rc=0, the green pytest count, pyright's zero errors, or
      the JSON family's verdict-free measurements);
    * ``"unparsable"`` — no verdict shape the door parses (a verdict
      shape may be new — TEACH the DOOR, never teach the capture to be
      silent).
    """
    if path.suffix == ".json":
        # The JSON families: the body is loaded (a torn/unparsable body
        # is the warning) and its verdict fields are read — the suffix
        # alone never greens a failure-shaped body.
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
    """The
    effective claim's capture clocks — the registry's ``captured_at``
    and the run-scoped filename's embedded stamp, where present — are
    PARSED and judged against the reader's clock. A capture dated in
    the FUTURE beyond the tolerance is the REFUSAL: no honest run
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
            "THE REFUSAL, never the green."
        )


def _content_door(
    stem: str,
    entry: dict[str, str],
    runs_dir: Path,
    failures: list[str],
    warnings: list[str],
) -> None:
    """CONTENT DOOR at the claim that greens the stem: the
    effective capture's own verdict is parsed and checked AGAINST the
    claim. A capture whose content records failure while the claim
    greens the stem is the CONTENT-VERDICT MISMATCH (a refusal);
    a capture with no parsable verdict is the TYPED WARNING (never a
    silent pass). Cheap by construction: a linear verdict-shape scan
    over the capture's text, no structured parse (the runtime
    record)."""
    base = runs_dir if runs_dir.is_absolute() else REPO / runs_dir
    path = _entry_capture_path(base, stem, entry["file"])
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
        # zero claims is zero verdicts — the loop over zero stems proves
        # nothing. The refusal carries the warning-class content (the
        # bootstrapping case is real) inside the nonzero verdict: the state is
        # honest, the verdict is never the green.
        return (
            [
                f"{runs_dir / 'CLAIMS.json'}: the claims registry is EMPTY — zero "
                "claims, zero verdicts: an estate with no claims CANNOT verify "
                "anything, and the law never greens on no evidence (the vacuous pass "
                "is the refusal this refusal kills). WARNING (empty-estate): the "
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
    # last entry of a stem is the live claim. No filename is parsed: the
    # manifest is the index (the name only ever points at the file).
    stems: dict[str, list[dict[str, str]]] = {}
    # The registry entry per capture path (the chain resolver's index):
    # the same file may be claimed by several stems — the registry's
    # append order decides (the last claim of a file owns the walk).
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
        by_path[str(_entry_capture_path(runs_dir, stem, file))] = entry
    failures: list[str] = []
    warnings: list[str] = []
    for stem, entries in sorted(stems.items()):
        live = entries[-1]
        marked = live.get("superseded_by")
        if marked:
            # walk the supersession chain to its terminal entry, then judge the
            # terminal by the head-sha rule. Every hop's content is checked (a
            # chain through a missing capture proves nothing), the walk must
            # terminate, and the termination point must be a real, fresh
            # capture — the effective claim is the chain's end.
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
            # content first (the terminal's capture must exist), then freshness
            # — the same rule a live claim carries, applied where the chain
            # actually lands.
            base = runs_dir if runs_dir.is_absolute() else REPO / runs_dir
            if not _entry_capture_path(base, stem, terminal["file"]).is_file():
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
                continue  # the terminal is fresh — the chain lands on this head
            if recorded is not None and _off_by_one_fresh(recorded, stem):
                # The off-by-one rule at the terminal (with its ancestry
                # teeth): the terminal's claimed head is an ancestor of
                # head and nothing behavioral changed since it (the diff
                # half's exclusions, per this stem — the docs-derived
                # stems' narrow set makes a docs-only merge stale) — the
                # capture verifies a content-identical tree.
                _capture_time_door(stem, terminal, failures)
                _content_door(stem, terminal, runs_dir, failures, warnings)
                continue
            if str(terminal.get("kind", "")).startswith("CITED-IMPORT"):
                # The cited-import declaration at the terminal: the chain
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
        missing = _entry_capture_path(base, stem, live["file"])
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
            continue  # the live claim is fresh — verified on its own head
        if recorded is not None and _off_by_one_fresh(recorded, stem):
            # The off-by-one rule (with its ancestry teeth): the claimed
            # head is an ancestor of head and nothing behavioral changed
            # since it (the non-behavioral estate — docs, the
            # readme/license/metadata class — cannot stalify a claim,
            # except for the docs-derived stems, whose batteries read
            # the docs estate: their narrow set makes a docs-only merge
            # stale) — the source tree the claim verifies is
            # content-identical. Fresh.
            _capture_time_door(stem, live, failures)
            _content_door(stem, live, runs_dir, failures, warnings)
            continue
        if str(live.get("kind", "")).startswith("CITED-IMPORT"):
            # the artifact is an imported provenance record — the imported
            # record is the provenance (``_sweep.py``'s curve cites it as
            # such). It is not a live claim of this tree and cannot be
            # re-recorded; the source claim built on it is a doc claim whose
            # substance is owned by the scope pin. History by its own
            # declaration.
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


def _install_signal_handlers() -> None:
    """SIGTERM and SIGHUP become SystemExit(128+signum): ``main``'s
    finally runs on their arrival, so the staging it holds is removed
    like any other exit. SIGKILL has no handler — the startup prune is
    its backstop."""

    def _raise_exit(signum: int, frame: object) -> None:
        raise SystemExit(128 + signum)

    for name in ("SIGTERM", "SIGHUP"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, _raise_exit)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, default=None)
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
    _install_signal_handlers()
    pruned = _prune_stale_staging()
    if pruned:
        print(
            f"pruned {len(pruned)} staging director{'y' if len(pruned) == 1 else 'ies'} "
            "left by dead runs (older than an hour, lock unheld)"
        )
    staging: Path | None = None
    staging_lock: IO[str] | None = None
    try:
        # the explicit --runs-dir wins always (the pins' synthetic estates':
        # test_evidence_content_door_pins builds a tmp estate and passes it —
        # the default face below must never override an explicit argument;
        # the override was the bug that verified the evidence branch's real
        # estate against a synthetic run and returned rc=0 for it).
        if args.runs_dir is not None:
            runs_dir: Path = args.runs_dir
        elif args.evidence_ref:
            runs_dir, staging, staging_lock = _stage_evidence_ref(args.evidence_ref)
        elif not (RUNS / "CLAIMS.json").is_file() and _evidence_ref_resolves(DEFAULT_EVIDENCE_REF):
            # no local registry (the tree stopped carrying it) — read the
            # evidence branch's estate.
            runs_dir, staging, staging_lock = _stage_evidence_ref(DEFAULT_EVIDENCE_REF)
        else:
            runs_dir = RUNS
        head, refusal = _verified_head()
        if refusal is not None:
            # the env's claim failed the cross-check — the refusal, never rc=0.
            print(f"THE HEAD-STAMP LAW REDS (head {head[:12]}):")
            print(f"  {refusal}")
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
    finally:
        # The staging copy is this invocation's scratch: the reads above
        # are done, and the expanded estate must not outlive the run.
        if staging is not None:
            shutil.rmtree(staging, ignore_errors=True)
        if staging_lock is not None:
            staging_lock.close()


if __name__ == "__main__":
    sys.exit(main())
