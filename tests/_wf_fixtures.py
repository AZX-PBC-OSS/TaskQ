"""Fixtures + seed helpers for the workflow pin suites, registered from the
root conftest.

These fixtures live here instead of ``tests/workflows/conftest.py`` for the
same reason ``tests/web_admin/_fixtures.py`` does: pytest 9.1.1 drops a
nested conftest's fixtures for a file revisited non-adjacently in the
argument list (pytest-dev/pytest#14971). The workflow pins span four files
(the engine's finalize/sweep/fork families, the schema's structural +
band families, the ledger family), so the seed helpers and the red-log
sink live here ONCE — a fixture duplicated across two files is a seam owed
now, and this is the seam's home.

Every helper drives the REAL engine surfaces (``taskq.workflows``); the
seed shapes are the flow-row/join-row/running-node representations the
engine's own statements read back.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections.abc import AsyncIterator, Callable, Iterator
from hashlib import sha256
from pathlib import Path
from typing import Any, Final, cast
from uuid import (
    uuid4,  # noqa: TID251  # Why: the redlog RUN ID is deliberately NOT a persisted id — no B-tree, no ordering; randomness is the point (attribution token).
)

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing._claims import load_claims, record_claim
from taskq.workflows import Promise
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.engine import render_workflow_sql

MEASUREMENTS = Path(__file__).parent.parent / ".measurements"


def runtime_refusal_builder(fn: Callable[[], object]) -> Callable[[], Promise[object]]:
    """THE NEGATIVE PROBE'S NAMED SEAM (the typed door's runtime face):
    the ``@app.workflow`` decorator takes ``Callable[[], Promise[R]]`` —
    a build function whose runtime return is NOT a promise (a bare dict,
    a bare ``None``) is refused STATICALLY at the decoration site (the
    probe corpus pins that refusal). The RUNTIME refusals those lying
    declarations used to reach — the build door's "the return IS the
    terminal promise" ``TypeError``, the E2 produced-never-consumed
    verdict — are still runtime behavior worth pinning: the probes
    construct the static-impossible shape through THIS seam, one cast,
    named and commented, so the checker sees the lie typed and the
    runtime door keeps its pin."""
    return cast("Callable[[], Promise[object]]", fn)


#: THE OFF-BY-ONE RULE'S NARROW EXCLUSION SET: the paths the staleness
#: diff ignores for EVERY stem — the measurements estate and the
#: workflow estate (.github). This is the DOCS-DERIVED stems' set (see
#: ``DOCS_SENSITIVE_STEM_PREFIXES``): for the evidence kinds whose
#: batteries READ the docs estate, a docs-only merge IS a source
#: change.
OFF_BY_ONE_NARROW_EXCLUSIONS: tuple[str, ...] = (
    ":(exclude).measurements",
    ":(exclude).github",
)

#: THE OFF-BY-ONE RULE'S EXCLUSION SET (the docs-exemption round): the
#: pathspecs the staleness diff IGNORES for every other stem — the
#: narrow set above plus the NON-BEHAVIORAL paths (docs and the
#: readme/license/metadata class: paths that cannot alter what a plain
#: stem's claim verifies). This is the in-suite mirror of the
#: verifier's own ``_NONBEHAVIORAL_EXCLUSIONS``
#: (``scripts/verify_evidence_heads.py``) — one law, two faces; the sync
#: pin (``tests/test_evidence_docs_exemption_pins.py``) holds them in
#: step. Pathspec shapes (git's anchoring, verified against git 2.56): a
#: directory pathspec covers its SUBTREE from the root; ``README*`` and
#: the ``glob``-magic ``*.md`` match at the ROOT only (glob magic: ``*``
#: does not cross ``/``) — a nested ``tests/*.md`` receipt still
#: stalifies.
OFF_BY_ONE_DIFF_EXCLUSIONS: tuple[str, ...] = (
    *OFF_BY_ONE_NARROW_EXCLUSIONS,
    ":(exclude)LEDGER.md",
    ":(exclude)docs",
    ":(exclude)README*",
    ":(exclude,glob)*.md",
    ":(exclude)LICENSE",
    ":(exclude).gitignore",
)

#: THE DOCS-DERIVED STEMS (the review round's cure) — the in-suite
#: mirror of the verifier's own ``_DOCS_SENSITIVE_STEM_PREFIXES``: the
#: evidence kinds whose batteries READ the docs estate (mkdocs builds
#: ``docs/`` strict; the docs-example tests'
#: ``tests/_docs_examples.py::DOC_ROOTS = ("docs", "README.md")``
#: EXECUTE the fences those documents carry; the docs-lane captures run
#: the same lanes). For these stems the docs exemption does not apply:
#: ``source_changes_since`` judges them on the NARROW set, and a
#: docs-only merge stalifies them exactly as a src/ merge would.
DOCS_SENSITIVE_STEM_PREFIXES: tuple[str, ...] = (
    "mkdocs-strict",
    "docs-examples",
    "docs-lane",
)


def source_changes_since(
    head_sha_value: str, stem: str | None = None, *, anchor: str | None = None
) -> bool:
    """THE OFF-BY-ONE RULE (the head-stamp verifier's own, mirrored for
    the in-suite guards): whether ANY commit since *head_sha_value*
    touched a BEHAVIORAL path. For a plain stem that is anything
    outside the measurements estate and the non-behavioral paths
    (``OFF_BY_ONE_DIFF_EXCLUSIONS`` above): a claim recorded on H is
    fresh at H2 when H2 is H plus measurement-and-docs-only changes —
    the source + test tree the claim verifies is content-identical.

    *anchor* names the UPSTREAM the freshness diff runs against: the
    run's own HEAD when ``None`` (the batteries' own law — the claim's
    reader IS the tree the claim verifies), or the evidence branch's
    ref head (the estate-sourced records' home tree: their liveness is
    a property of the tree THEY verified, not of the checkout that
    happens to be reading them).

    THE DOCS-DERIVED STEMS' FALLBACK (the review round's cure): *stem*
    carrying a ``DOCS_SENSITIVE_STEM_PREFIXES`` prefix names an
    evidence kind whose battery READS the docs estate — mkdocs builds
    ``docs/`` strict and the docs-example tests' ``DOC_ROOTS`` execute
    the ``docs/`` + ``README.md`` fences — so for those the exemption
    set is the NARROW one (``OFF_BY_ONE_NARROW_EXCLUSIONS``) and a
    docs-only merge stalifies the claim exactly as a src/ merge would.

    An unresolvable claimed head (not an ancestor) = stale by
    definition.

    THE PER-RUN MEMO (the estate's scale — 18k+ sink records across
    ~100 distinct claimed heads): the verdict is a pure function of the
    claimed sha, THE STEM'S exclusion set and THIS RUN's HEAD — and
    HEAD cannot move mid-run (a test process never commits; the run's
    git world is fixed), so each distinct (claimed sha, stem) is
    resolved AT MOST ONCE per process. The guard's walk over every
    sink record collapses from O(records) git subprocesses (18k+ — the
    per-test timeout's knife edge at 300 s, convicted: the wf battery's
    redlog-guard timeout) to O(distinct heads). THE MEMO KEY IS THE
    PAIR (the review round's own correctness fix): a docs-derived stem
    and a plain stem sharing a claimed head get DIFFERENT verdicts from
    DIFFERENT exclusion sets — keyed by the sha alone the first caller
    would answer for both."""
    import subprocess

    memo_key = (head_sha_value, stem, anchor)
    cached = _SOURCE_CHANGES_MEMO.get(memo_key)
    if cached is not None:
        return cached

    if stem is not None and stem.startswith(DOCS_SENSITIVE_STEM_PREFIXES):
        exclusions = OFF_BY_ONE_NARROW_EXCLUSIONS
    else:
        exclusions = OFF_BY_ONE_DIFF_EXCLUSIONS
    proc = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            f"{head_sha_value}..{anchor or 'HEAD'}",
            "--",
            ".",
            *exclusions,
        ],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    verdict = True
    if proc.returncode == 0:
        verdict = bool(proc.stdout.strip())
    _SOURCE_CHANGES_MEMO[memo_key] = verdict
    return verdict


#: The memo's store (see :func:`source_changes_since` — the verdict is
#: pure in (claimed sha, the stem's exclusion set, the anchor); the key
#: is the TRIPLE — the docs-derived stems' narrow set and a plain stem's
#: full set disagree on the same sha, and the estate anchor disagrees
#: with the run's own HEAD).
_SOURCE_CHANGES_MEMO: dict[tuple[str, str | None, str | None], bool] = {}


def head_sha() -> str:
    """THE HEAD-STAMP LAW (the evidence-integrity round, cure 5): every
    evidence artifact RECORDS ITS HEAD — the sha the verification
    (``scripts/verify_evidence_heads.py``) re-runs the artifact against.
    An unstamped capture is a rumor; a stale one is history, never the
    live claim."""
    import subprocess

    out = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return out.strip()


#: The provenance law's version marker — a sink record stamped with this
#: law carries FULLY-CITED entries (run + capture path + sha256); the
#: guard verifies every record stamped with it and grandfathered NOTHING
#: forward: a pre-law record is history, never the live claim.
REDLOG_LAW = "provenance-1"


#: The environment variable that pins the redlog captures' directory.
#: The battery that records evidence for the measurement estate sets it
#: to the in-repo path (``.measurements/redlog-captures``) so the
#: captures are harvested onto the evidence branch together with the
#: sinks that cite them; unset, the captures go to the system temp dir.
REDLOG_CAPTURES_DIR_ENV = "TASKQ_REDLG_CAPTURES_DIR"


def redlog_captures_root() -> Path:
    """The directory the redlog drills' capture files are written to.

    Default: ``taskq-redlog-captures-<uid>`` under the system temp dir
    (``tempfile.gettempdir()``, which honors ``TMPDIR``), created
    private to the user (0700) — the captures are receipts, and a
    shared-world-readable temp directory is neither. The path is
    resolved to its absolute form so a reader in another working
    directory resolves the same directory. The captures are per-run
    scratch receipts; writing them into the repository tree left every
    worktree littered with untracked per-run directories after each
    pytest run. The explicit pin (``TASKQ_REDLG_CAPTURES_DIR``) moves
    the root back into the tree for the battery that records evidence
    for the measurement estate.
    """
    pinned = os.environ.get(REDLOG_CAPTURES_DIR_ENV)
    if pinned:
        return Path(pinned).resolve()
    return (Path(tempfile.gettempdir()) / f"taskq-redlog-captures-{os.getuid()}").resolve()


def _repo_root_containing(start: Path) -> Path | None:
    """The nearest ancestor of *start* carrying ``pyproject.toml`` or
    ``.git`` (a file in a worktree). ``None`` when no ancestor does."""
    for ancestor in (start, *start.parents):
        if (ancestor / "pyproject.toml").is_file():
            return ancestor
        git = ancestor / ".git"
        if git.exists():
            return ancestor
    return None


#: The GUARD'S OWN CHECKOUT — the tree carrying this file, resolved
#: (an unresolved ``__file__`` through a symlinked checkout anchors the
#: walk at the symlink's view, and a record root resolved to the real
#: tree escapes it). The trust rule anchors there (never at the record's
#: or the estate's location): a staged estate (no local
#: ``.measurements``) lives in the temp dir, where no ancestor carries a
#: repo marker — anchoring there would find no repository and trust an
#: in-repo record root again. Anchored at the checkout, the in-repo
#: refusal holds identically for a staged estate and a local one.
_REPO_ROOT: Path | None = _repo_root_containing(Path(__file__).resolve())


def resolve_redlog_capture(
    measurements: Path, capture_rel: str, captures_root: str | None = None
) -> tuple[Path, list[str], list[str]]:
    """Locate a capture cited by a redlog sink entry.

    Returns ``(capture, homes_searched, refused_roots)``: *capture* is
    the first existing trusted candidate (the last trusted candidate
    when none exists, so the missing-file verdict names a home);
    *homes_searched* names every trusted root; *refused_roots* names
    the record-declared roots the trust rule rejected.

    THE TRUST RULE for candidate roots: the estate root being read by
    the guard, the ``TASKQ_REDLG_CAPTURES_DIR`` pin, and the default
    temp root are trusted. The record's own captures_root is trusted
    only when it lies OUTSIDE the guard's own checkout (``_REPO_ROOT``,
    the tree carrying this file — the anchor is the GUARD's home, never
    the record's or the estate's, so a staged estate whose temp
    location carries no repo marker refuses identically): a record is
    data, and a record pointing its receipts into the checkout under
    review would let that tree ship the capture that clears its own
    receipt chain. The estate's own namespace still reaches an in-repo
    pinned battery's captures (they live under the estate root), and a
    battery whose captures went to an external directory resolves
    through its record's root.

    A citation that is absolute or carries ``..`` raises ValueError —
    as does a candidate that resolves outside the root it was joined
    to (a symlink planted inside would walk the read out). The caller
    turns the refusal into its own named violation.
    """
    rel_path = Path(capture_rel)
    if not capture_rel or rel_path.is_absolute() or ".." in rel_path.parts:
        raise ValueError(
            f"the cited capture path {capture_rel!r} is not an "
            "estate-relative path (absolute, empty, or carrying '..'): it "
            "points outside the estate, and a capture outside the estate "
            "proves nothing"
        )
    rel_under_captures = capture_rel.removeprefix("redlog-captures/")

    candidates: list[tuple[Path, Path]] = []
    refused_roots: list[str] = []
    if captures_root:
        declared = Path(captures_root).resolve()
        if _REPO_ROOT is not None and declared.is_relative_to(_REPO_ROOT):
            refused_roots.append(str(declared))
        else:
            candidates.append((declared, declared / rel_under_captures))
    pinned = os.environ.get(REDLOG_CAPTURES_DIR_ENV)
    if pinned:
        root = Path(pinned).resolve()
        candidates.append((root, root / rel_under_captures))
    estate_root = measurements.resolve()
    candidates.append((estate_root, measurements / capture_rel))
    default_root = redlog_captures_root()
    candidates.append((default_root, default_root / rel_under_captures))

    homes = [str(root) for root, _ in candidates]
    for root, candidate in candidates:
        if not candidate.resolve().is_relative_to(root):
            raise ValueError(
                f"the cited capture path {capture_rel!r} resolves to "
                f"{candidate.resolve()} — outside its root {root}: a capture "
                "outside the estate proves nothing"
            )
        if candidate.is_file():
            return candidate, homes, refused_roots
    return candidates[-1][1], homes, refused_roots


def write_band_artifact(name: str, payload: dict[str, object]) -> Path:
    """THE APPEND-ONLY CONVERSION (the receipts law): a band artifact is
    a RUN-SCOPED file, never a write_text-in-place name. The convicted
    defect: every run rewrote the SAME file whole — two concurrent runs'
    writes tore each other (the 11 torn rows the ledgers' union found),
    and a partial run's numbers falsified the recorded band (the newest
    subset is not the newest measurement). Each run writes
    ``runs/{stem}-{timestamp}-{token}.json`` — AND APPENDS THE CLAIM to
    the registry (``CLAIMS.json``; :func:`record_claim`) — the newest
    CITED (:func:`latest_band_artifact`) is the record the report reads.
    The filename convention stays for the humans; the claim lives in the
    manifest (the machinery never parses the name)."""
    runs = MEASUREMENTS / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    stem = name.removesuffix(".json")
    path = runs / f"{stem}-{time.strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:4]}.json"
    record = dict(payload)
    # THE HEAD-STAMP LAW: the artifact carries the head it was measured
    # on — the verification (scripts/verify_evidence_heads.py) re-runs it
    # on its claimed head.
    record.setdefault("head_sha", head_sha())
    path.write_text(json.dumps(record, indent=2, default=str))
    # THE CLAIMS REGISTRY: the manifest is the estate's index — the
    # verifier reads it, never the filename's run-scoped tail.
    record_claim(stem=stem, file=path.name, head_sha=str(record["head_sha"]))
    return path


def latest_band_artifact(stem: str) -> Path | None:
    """The newest CITED: the newest run-scoped capture for *stem* (the
    registry's append-order tail for the stem — the record a report
    cites). Read off the CLAIMS manifest, never off the filenames."""
    claims = [c for c in load_claims() if c.get("stem") == stem]
    for claim in reversed(claims):
        candidate = MEASUREMENTS / "runs" / claim.get("file", "")
        if candidate.is_file():
            return candidate
    return None


# ── THE LOADED BAR (the load-flake band's discipline — finding 3's cure) ─

#: The bar: ambient load (1-min run-queue average) per core at or below
#: which the box counts QUIET and a wall-clock band may assert honestly.
#: Above it, an absolute wall-clock bound measures the NEIGHBORS, not the
#: code under test — the pin that greened solo and flaked loaded (the
#: census's own record: "green solo x2" while the box sat at 61/72).
LOADED_BAR_PER_CORE: Final[float] = 0.5

#: The loaded round's honesty scale: a deadline polled under load stretches
#: by at most this factor (the box at 2x saturation gets 2x the bound —
#: condition-not-clock: the CONDITION scales the clock, never a fixed
#: number pretending the machine is quiet).
LOADED_SCALE_MAX: Final[float] = 4.0


def load_bar() -> float:
    """The box's ambient load, normalized per core (the loaded bar's
    reading). Condition-not-clock: an absolute wall-clock band asserts
    only when THIS reading is quiet."""
    import os

    return os.getloadavg()[0] / (os.cpu_count() or 1)


def loaded_scale() -> float:
    """The polling scale the current load earns: 1.0 on a quiet box,
    growing linearly to :data:`LOADED_SCALE_MAX` at 4x saturation. A
    poll's deadline multiplies by this — the poll waits on STATE
    (condition-not-clock), and the wait's BOUND acknowledges the box it
    runs on (state-not-sleep: the poll reacts to the observed state each
    tick, never a fixed sleep pretending to know when the state lands)."""
    bar = load_bar()
    if bar <= LOADED_BAR_PER_CORE:
        return 1.0
    return min(1.0 + (bar - LOADED_BAR_PER_CORE), LOADED_SCALE_MAX)


def band_assertion_allowed() -> bool:
    """Whether an ABSOLUTE wall-clock band may assert on this box, now:
    the loaded-bar gate. A loaded box waives the absolute bound honestly
    (the artifact records the waiver and the reading) — the RELATIVE
    drills (red vs. green) assert unconditionally, load-invariantly."""
    return load_bar() <= LOADED_BAR_PER_CORE


#: The terminal-status SQL set — the statement-side literal the engine's
#: guards spell (the twin of statemachine.TERMINAL_STATUSES); the pins'
#: convicted variants re-spell it.
TERMINAL_SQL = "('succeeded','failed','cancelled','crashed','abandoned')"


class RedLog:
    """The red-output sink: a file that gets READ (BUILD-PROTOCOL §2).
    Every pin's convicted variant appends its observed dragon here; the
    fixture flushes on teardown.

    THE PRESERVATION LAW (attack-3's hygiene finding, the fixer's own
    lane): the evidence sinks are APPEND-ONLY and RUN-SCOPED — flush()
    appends ONE JSONL record (``{"run": …, "entries": [...]}``) and
    never rewrites the file. The convicted defect: ``write_text``
    rewrote the WHOLE sink per run, so a PARTIAL run (one pin file
    re-run in isolation) replaced the corpus with ONLY that subset's
    entries — a full run's evidence silently deleted, the recorded band
    numbers falsified by whichever subset ran last. Append-only: a
    partial run adds its own run-scoped record; history is never
    truncated. The run id (pid + a token + the timestamp) makes each
    record attributable.

    THE PROVENANCE LAW (the evidence-integrity round, cure 3): every
    entry cites a RECEIPT CHAIN — (a) its drill's run-id, (b) the
    drill's captured output FILE PATH, (c) a sha256 of that file. The
    capture is written at RECORD time (the drill's own observation,
    machine-written, in the run's own capture directory); the guard
    (:mod:`tests.test_fv_redlog_guard`) re-verifies the chain at guard
    time — a cited capture that is missing or mismatching FAILS the
    guard. The law REPLACES the syntactic shape rule (the AST
    literal-only scan): that guard was defeatable twice over (a
    keyword-form payload skipped inspection; a name-laundered
    module-level constant walked past it) — the receipt chain inspects
    the SINK, never the call's shape, and convictions are by provenance.

    The captures are written outside the repository tree by default
    (see :func:`redlog_captures_root`) — per-run scratch receipts, not
    source. A battery recording evidence for the measurement estate
    pins them into ``.measurements/redlog-captures/`` with
    ``TASKQ_REDLG_CAPTURES_DIR`` so they are harvested onto the
    evidence branch together with the sinks that cite them.
    """

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}-{uuid4().hex[:8]}"
        self._captures_root = redlog_captures_root()
        self.entries: list[dict[str, str]] = []

    def red(self, pin: str, mutation: str, observed: Any) -> None:
        """Record one convicted red WITH its receipt: the entry carries
        the run-id, the drill's captured output file path, and the
        capture's sha256 — the chain the guard verifies. The capture's
        payload carries the same identity (the pin and the run), so a
        cited capture self-identifies with the entry citing it."""
        self._captures_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        capture_dir = self._captures_root / self._run_id
        capture_dir.mkdir(parents=True, exist_ok=True)
        sanitized = "".join(c if c.isalnum() or c in "-_" else "_" for c in pin) or "pin"
        capture = capture_dir / f"{sanitized}.json"
        payload = {
            "pin": pin,
            "mutation": mutation,
            "observed": observed,
            "run": self._run_id,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        capture.write_text(json.dumps(payload, indent=2, default=str, sort_keys=True))
        digest = sha256(capture.read_bytes()).hexdigest()
        self.entries.append(
            {
                "pin": pin,
                "mutation": mutation,
                "red": json.dumps(observed, default=str),
                "run": self._run_id,
                # The citation names the path under the estate root —
                # the shape every reader resolves (see
                # resolve_redlog_capture), wherever the captures'
                # physical root sits.
                "capture": f"redlog-captures/{self._run_id}/{sanitized}.json",
                "sha256": digest,
            }
        )

    def flush(self) -> None:
        MEASUREMENTS.mkdir(exist_ok=True)
        record = {
            "run": self._run_id,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "law": REDLOG_LAW,
            # THE HEAD-STAMP LAW: the sink record carries the head the
            # drills ran on (the receipt chain's fourth link — the run
            # is attributable to a TREE, not just a process).
            "head_sha": head_sha(),
            "entries": self.entries,
            # Where this run's cited captures live physically: the
            # default root is the system temp dir, which a reader can
            # have under a different TMPDIR (or a tmpfs reaper can have
            # emptied), so the record carries its own — the resolver's
            # most specific candidate. Absent on records written before
            # the captures left the repository tree.
            "captures_root": str(self._captures_root),
        }
        with (MEASUREMENTS / self._filename).open("a") as sink:
            sink.write(json.dumps(record) + "\n")


@pytest.fixture
def engine_redlog() -> Iterator[RedLog]:
    """The red sink for the engine pins (one file, read by the gate)."""
    log = RedLog("pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def ledger_redlog() -> Iterator[RedLog]:
    """The red sink for the ledger pins."""
    log = RedLog("ledger-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def hitl_redlog() -> Iterator[RedLog]:
    """The red sink for the T10 HITL pins (the convicted variants)."""
    log = RedLog("t10-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def loop_redlog() -> Iterator[RedLog]:
    """The red sink for the T19 loop pins (the mutation drills' reds)."""
    log = RedLog("t19-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def propagation_redlog() -> Iterator[RedLog]:
    """The red sink for the T06 propagation pins (the phase-2 family)."""
    log = RedLog("t06-propagation-reds.json")
    yield log
    log.flush()


@pytest.fixture
def t20_redlog() -> Iterator[RedLog]:
    """The red sink for the T20 streaming pins (the convicted variants)."""
    log = RedLog("t20-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def progress_redlog() -> Iterator[RedLog]:
    """The red sink for the T21 progress pins (the convicted variants)."""
    log = RedLog("t21-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def createseam_redlog() -> Iterator[RedLog]:
    """The red sink for the create-seam pins (the create's atomicity, the
    run-key claim's honesty, the root-marker fence, the reap belt, the
    packaged run)."""
    log = RedLog("createseam-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def wedge_redlog() -> Iterator[RedLog]:
    """The red sink for the crashed-terminal wedge pins (the pre-cure
    tree's corpse-greens: the reconstruction's running, the maintenance
    pass's non-heal, the G7 blind spot)."""
    log = RedLog("wedge-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def timer_redlog() -> Iterator[RedLog]:
    """The red sink for the T28 durable-timer pins (the pre-implementation
    reds + the convicted variants)."""
    log = RedLog("t28-timer-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def versioning_redlog() -> Iterator[RedLog]:
    """The red sink for the T29 versioning-contract pins (the redeploy
    face's reds)."""
    log = RedLog("t29-versioning-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def timer_cures_redlog() -> Iterator[RedLog]:
    """The red sink for the timer-cures lane's pins (the six cures' reds:
    the trojan prefix, the broadcast leak, the explorer's template, the
    10k tail, the versioning wedge's surface, the naive-datetime doc)."""
    log = RedLog("timer-cures-pin-reds.json")
    yield log
    log.flush()


#: The G7 always-on assertion's mapping: the §17.5 derivation's workflow
#: status → the flow ROOT row's job_status (the root's legal vocabulary).
#: The mapping is the TERMINAL states' expectation; the LAW is stated in
#: :func:`g7_check` (the root row is a cache, and each terminal root has
#: exactly one writer whose semantics decide what it may claim).
G7_DERIVED_TO_ROOT: dict[str, str] = {
    "complete": "succeeded",
    "failed": "failed",
    "cancelled": "cancelled",
    "blocked": "running",
    "running": "running",
    "pending": "pending",
}


@pytest.fixture
async def wf_g7_status_truth(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> AsyncIterator[None]:
    """G7 (T08): the ALWAYS-ON metamorphic assertion — at test end, the
    REPORTED workflow status (the flow root row's own status, the
    linearization point the engine's maintenance leg writes) equals the
    status RECONSTRUCTED FROM ROWS ALONE (the §17.5 derivation over the
    node rows + the ledger — D4's two-source rule). Registered for every
    workflow integration test via the collection hook (tests/conftest.py
    adds it to the wf-family files); catches status-drift continuously,
    not just in the dedicated pin's scenarios. A deliberately-lying
    fixture status (a root hand-written to a state the rows cannot
    derive) reds the suite (the drill pin proves the teeth)."""
    yield
    await g7_check(wf_conn, wf_schema, wf_sql)


async def g7_check(wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql) -> None:
    """The G7 assertion's body (one home — the fixture and the teeth-drill
    pin both run THIS, never a re-spelled copy).

    THE LAW (the cache's semantics, stated from each terminal root's
    writer — the phase-2 attack's H4 round made this check ALWAYS-ON for
    real, so the law must red lies and never red the flips' windows):

    * the root row is a CACHE of the derivation;
    * a root claiming 'succeeded' is written by ONE writer only — the
      maintenance leg's complete branch (all rows terminal, none failed,
      none cancelled) — so the rows MUST derive 'complete'. A
      prematurely-complete root over rows that derive anything else is
      THE status-cache lie this check exists to catch (the teeth pin's
      own lie is this shape);
    * a root claiming 'failed' may never contradict a COMPLETED run (the
      cascade and the maintenance leg both write it from a non-absorbed
      failure on the rows; the rows cannot un-fail);
    * a root claiming 'cancelled' is the cancel arm's linearization
      point — the rows may lag it in every direction (a straggler child
      terminal-failing after the flip derives 'failed'; the flip
      stands) — nothing to assert against;
    * a LIVE root ('running'/'pending') may lag the rows' terminal
      verdict by one sweep pass — the maintenance leg's lag window (the
      H1 cure heals it in one pass; the H1 pin carries that teeth).

    THE WEDGE LEG (the crashed-terminal wedge's red-drill — the
    attacker's proof was G7 staying green ON THE CORPSE): a live root
    whose rows reconstruct a TERMINAL verdict contradicts the rows.
    The lag window is ONE pass, so the check RUNS the maintenance leg
    itself (the same statement the sweep drives) and re-reads: the
    reported state must now equal the rows' terminal verdict. A root
    that still disagrees is THE WEDGE — a run the reported state claims
    is live while the rows say it terminalized and nothing will ever
    change either side (the corpse that reports 'running' forever). The
    drill pin (tests/test_wf_t20_crashed_terminal_wedge.py) reds this
    leg against a no-op maintenance mutant — the assertion can fail on
    a wedge, it is not a decoration."""
    from taskq.workflows._status import reconstruct_workflow_status

    flows = await wf_conn.fetch(
        f"SELECT id, status FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__'"
    )
    for flow in flows:
        reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(flow["id"]))
        status = flow["status"]
        if status == "succeeded":
            assert reconstructed == "complete", (
                f"the reported status drifted from the rows: flow {flow['id']} "
                f"reports 'succeeded' but the rows reconstruct "
                f"{reconstructed!r} — the status cache has arrived (G7): a "
                "prematurely-complete root is written by nothing but a "
                "cache"
            )
        elif status == "failed":
            assert reconstructed != "complete", (
                f"the reported status drifted from the rows: flow {flow['id']} "
                f"reports 'failed' but the rows reconstruct 'complete' — "
                "the wrong verdict on a completed run (G7)"
            )
        elif status in ("running", "pending") and reconstructed in (
            "failed",
            "cancelled",
            "complete",
        ):
            # THE WEDGE LEG: the rows have spoken a TERMINAL verdict; the
            # lag window is one sweep pass — run it, re-read, and the
            # root must agree. (The maintain statement's own derivation
            # re-derives the root from the node rows — a disagreement
            # AFTER the pass is the two derivations' drift or a wedging
            # sweep.) ONE HONEST LAG SURVIVES the leg: the §17.5
            # cancelled/failed PRECEDENCE derives a terminal verdict OVER
            # live rows (a fired-but-unclaimed join row is claimable
            # work the derivation's cancelled row outranks) — the root
            # legitimately waits for it. THE WEDGE is the ALL-TERMINAL
            # corpse: no live-class row exists anywhere and the root
            # still reports live — nothing will ever change either side.
            await wf_conn.execute(wf_sql.workflow_root_maintain, 200)
            healed = await wf_conn.fetchval(
                f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow["id"]
            )
            expected = G7_DERIVED_TO_ROOT[reconstructed]
            if healed == expected:
                continue
            remaining = await wf_conn.fetchval(
                f"""SELECT count(*) FROM "{wf_schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1
                      AND metadata ? 'flow_id'
                      AND step_key <> '__flow__'
                      AND status IN ('pending', 'scheduled', 'running')""",
                flow["id"],
            )
            if remaining:
                # THE HONEST PRECEDENCE LAG: the terminal verdict derives
                # OVER live rows (the fired-but-unclaimed join is
                # claimable work) — the root waits for them, the law's
                # lag window covers it.
                continue
            assert healed == expected, (
                f"THE WEDGE (G7's terminal-contradiction leg): flow "
                f"{flow['id']} reports {healed!r} while the rows "
                f"reconstruct {reconstructed!r} with NO live-class row "
                f"left on the run — a TERMINAL verdict the one-pass heal "
                f"(the maintenance leg) did not land. The run's reported "
                "state contradicts its own rows AND the heal: a wedged "
                "run — the operator must notice forever (the "
                "crashed-terminal wedge's signature: the corpse reports "
                "'running' over a deterministic death)"
            )


@pytest.fixture
async def wf_conn(clean_pg_conn: asyncpg.Connection) -> asyncpg.Connection:
    """The per-test clean connection on the module's migrated schema."""
    return clean_pg_conn


@pytest.fixture
def wf_schema(module_pg_schema: Any) -> str:
    return module_pg_schema.schema_name


@pytest.fixture
async def wf_pool(module_pg_schema: Any) -> AsyncIterator[asyncpg.Pool]:
    """The RUNNER's pool (T09): the flow runner acquires from a pool (its
    claims + finalizes); the module's DSN builds it — one pool per test,
    closed at teardown. The loop pins (T19) share it (a fixture
    duplicated across two files is a seam owed NOW — this is the
    seam's home, beside the other wf fixtures)."""
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    yield pool
    await pool.close()


@pytest.fixture
def wf_sql(module_pg_schema: Any) -> WorkflowSql:
    """The workflow statement bundle rendered for the module's schema."""
    return render_workflow_sql(module_pg_schema.schema_name)


class FlowStandIn:
    """The registered-definition stand-in (T09's API formalizes it; the
    run-key claim needs the placement fields only)."""

    def __init__(self, name: str = "ledger-flow") -> None:
        self.name = name
        self.actor = "wf"
        self.queue = "default"
        self.max_attempts = 3
        self.retry_kind = "transient"
        self.payload: dict[str, object] = {"flow": name}
        self.trace_id: str | None = None


async def seed_flow(
    conn: asyncpg.Connection, schema: str, *, status: str = "running", workflow: str | None = None
) -> JobId:
    """The flow-run row: a jobs row, step_key = the entry marker, the run
    scope. Its status IS the run's status (the linearization point).
    ``workflow`` stamps the root's metadata with the workflow's registered
    name — the sweep's fire arm resolves a healed join's reducer body from
    that REGISTERED DEFINITION (the durable, cross-process leg; the
    process-local memo is a cache). The stamp is what ``insert_flow_run``
    writes for a named flow; the seed passes it through so the pins drive
    the shipped shape."""
    flow_id = new_uuid()
    metadata: dict[str, object] = {"flow_id": str(flow_id)}
    if workflow:
        metadata["workflow"] = workflow
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', $2, "
        "'__flow__', $4::jsonb, 'workflow-run', $3)",
        flow_id,
        status,
        f"flow:{flow_id}",
        json.dumps(metadata),
    )
    return JobId(flow_id)


async def seed_join(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    step_key: str = "join",
    deps: int = 1,
    scheduled_in: float | None = None,
    consumers: list[dict[str, object]] | None = None,
) -> JobId:
    """A join-wait row: pending + deps_pending = deps + blocking_reason
    join. ``scheduled_in`` seconds in the future makes it a HELD row (P3
    rule 1's representation: the signal deadline is the only live timer)."""
    join_id = new_uuid()
    # VALUES expressions cannot reference the row's own columns -- the due
    # case is now() minus an hour, not the column minus an hour.
    scheduled_sql = (
        "now() - interval '1 hour'"
        if scheduled_in is None
        else f"now() + interval '{scheduled_in} seconds'"
    )
    meta: dict[str, object] = {"flow_id": str(flow_id), "blocking_reason": "join"}
    if consumers:
        meta["consumers"] = consumers
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata, scheduled_at) "
        f"VALUES ($1, 'wf', 'default', '{{}}', 3, 'transient', 'pending', $2, $3, "
        f"$4::jsonb, {scheduled_sql})",
        join_id,
        step_key,
        deps,
        json.dumps(meta),
    )
    return JobId(join_id)


async def seed_edge(
    conn: asyncpg.Connection, schema: str, child_id: JobId, parent_id: JobId, flow_id: JobId
) -> None:
    """One edge-ledger row (the join counter's ONLY truth source)."""
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        child_id,
        parent_id,
        flow_id,
    )


async def seed_running_node(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, *, step_key: str = "a"
) -> JobId:
    """A running node: the finalize fence's admitted shape (its claim view:
    a fresh worker, attempt 1, epoch 0)."""
    node_id = new_uuid()
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, locked_by_worker, lock_expires_at, "
        "claim_epoch, step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, $2, "
        "now() + interval '90 seconds', 0, $3, $4)",
        node_id,
        worker_id,
        step_key,
        json.dumps({"flow_id": str(flow_id)}),
    )
    return JobId(node_id)


async def claim_view(
    conn: asyncpg.Connection, schema: str, node_id: JobId
) -> tuple[JobId, int, int]:
    """The node's CURRENT claim view (worker, attempt, epoch) — what its
    own finalize presents to the fence."""
    rec = await conn.fetchrow(
        f'SELECT locked_by_worker, attempt, claim_epoch FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert rec is not None and rec["locked_by_worker"] is not None
    return JobId(rec["locked_by_worker"]), rec["attempt"], rec["claim_epoch"]


async def node_state(conn: asyncpg.Connection, schema: str, node_id: JobId) -> dict[str, Any]:
    """The node row's engine-relevant columns (metadata jsonb arrives as
    str on un-coded connections — decoded here)."""
    rec = await conn.fetchrow(
        f'SELECT status, deps_pending, metadata, result FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert rec is not None
    return {
        "status": rec["status"],
        "deps_pending": rec["deps_pending"],
        "metadata": rec["metadata"]
        if isinstance(rec["metadata"], dict)
        else json.loads(rec["metadata"] or "{}"),
        "result": rec["result"],
    }


async def fire_count(conn: asyncpg.Connection, schema: str, join_id: JobId) -> int:
    """How many fire rows the join has (exactly one = exactly-once)."""
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_join_fire WHERE join_job_id = $1',
            join_id,
        )
    )
