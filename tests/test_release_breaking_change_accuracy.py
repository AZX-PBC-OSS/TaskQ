"""The 0.3.0 breaking-change entries must be correct where release-please reads them.

release-please writes the ``⚠ BREAKING CHANGES`` section of a release PR
purely from conventional-commit markers (``type!:`` subjects or
``BREAKING CHANGE:`` footers) found across the commit range it walks --
never from hand-written prose and never from one nominated commit. The
truthfulness of the notes is therefore a property of that range.

Two range shapes matter, and this file asserts over each where it can:

* **The feeding range** — ``v<last-release>..HEAD`` (per
  ``.release-please-manifest.json``; the walk's depth over this range is
  pinned by ``test_breaking_change_markers.py``). Existence pins over
  this range ask: will the regenerated notes carry the corrected
  entries at all? An ancestor of this range (``4a5da1e``, published on
  main) carries a footer whose heartbeat_timeout framing was superseded
  before 0.3.0 shipped, and its text is immutable shared history -- so
  these pins cannot demand the whole range be clean; they demand a
  corrected entry exists alongside the stale one. Removing the stale
  bullet itself requires the one mechanism that rewrites what
  release-please reads for an already-merged commit: a
  ``BEGIN_COMMIT_OVERRIDE`` section in the associated pull request's
  body.
* **The branch's own commits** — ``<fork-point>..HEAD``. Quality pins
  over this range ask: is every breaking note THIS branch adds framed
  correctly? Everything here is mutable before merge, so every entry is
  held to the full contract.

Why one note per commit matters here: the parser surfaces at most ONE
breaking note per commit message, and which one survives a stack of
``BREAKING CHANGE:`` paragraphs depends on the paragraph layout -- the
corrected heartbeat and denial footers that ``77ccbb2`` landed were
silently dropped for exactly this reason (three run-together footer
lines keep only the last). The note extraction below therefore models
the parser's note-per-virtual-commit shape rather than counting raw
footer lines.

These pins retire themselves: they describe the pending 0.3.0 notes
specifically, and skip once the manifest moves past 0.2.2 (the release
PR and the post-cut main both carry 0.3.0). Delete this file when the
0.3.0 retro is no longer needed.

The tests read git history and doc prose -- never source structure --
so they hold across any refactor that leaves the documented behavior and
the range's marker text intact.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_UPGRADING = _REPO_ROOT / "docs" / "guides" / "upgrading.md"
_RELEASE_MANIFEST = _REPO_ROOT / ".release-please-manifest.json"

#: These pins describe the pending 0.3.0 release: the last released
#: version they sit on top of, and the tag that anchors the feeding
#: range.
_LAST_RELEASED = "0.2.2"

#: Conventional-commit breaking markers: ``type(scope)!:`` subjects, or the footers.
_SUBJECT_MARKER = re.compile(r"^[a-z]+(\([^)]*\))?!:")
_FOOTER_MARKER = re.compile(r"^BREAKING[ -]CHANGE:")
_NESTED_BLOCK = re.compile(r"BEGIN_NESTED_COMMIT\n(.*?)\nEND_NESTED_COMMIT", re.DOTALL)

#: Main-line ref candidates: a CI PR checkout (detached head) carries only
#: the remote-tracking ``origin/main``; a local checkout usually has both.
_MAIN_LINE_CANDIDATES = ("origin/main", "main")

#: Needles that identify the snooze/denial-accounting entry among the range's
#: breaking markers.
_DENIAL_NEEDLES = ("denial", "snooze", "job_attempts", "job_events", "max_attempts")


def _git(*args: str) -> str:
    result = subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
        check=True,
    )
    return result.stdout


def _merge_bases_with_main_line() -> list[str]:
    """The fork points HEAD shares with each resolvable main-line ref.

    A shallow or history-rewritten clone cannot answer ``merge-base`` (or
    resolve the refs at all); those candidates drop out here, and the
    module-level guard skips the pins when none survive.
    """
    bases: list[str] = []
    for candidate in _MAIN_LINE_CANDIDATES:
        resolved = subprocess.run(  # noqa: S603
            ["git", "merge-base", "HEAD", candidate],  # noqa: S607
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
        )
        if resolved.returncode == 0:
            base = resolved.stdout.strip()
            if base and base not in bases:
                bases.append(base)
    return bases


def _fork_point() -> str | None:
    """The newest merge-base: the branch's own range starts where the
    branch last shared history with the main line. A raw ref would do for
    a fresh checkout, but a stale local ``main`` is itself an ancestor of
    the real fork point -- taking the newest merge-base keeps the range to
    this branch's own commits in every environment."""
    bases = _merge_bases_with_main_line()
    for base in bases:
        if not any(
            other != base
            and subprocess.run(  # noqa: S603
                ["git", "merge-base", "--is-ancestor", base, other],  # noqa: S607
                capture_output=True,
                cwd=_REPO_ROOT,
            ).returncode
            == 0
            for other in bases
        ):
            return base
    return None


def _feeding_range() -> str | None:
    """``v<last-release>..HEAD`` — the range release-please feeds the
    pending release's notes from, or None when the tag is not resolvable
    in this checkout."""
    tag = f"v{_LAST_RELEASED}"
    resolved = subprocess.run(  # noqa: S603
        ["git", "rev-parse", "--verify", "--quiet", tag],  # noqa: S607
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
    )
    if resolved.returncode != 0:
        return None
    return f"{tag}..HEAD"


def _skip_reason() -> str | None:
    manifest = json.loads(_RELEASE_MANIFEST.read_text())
    released = str(manifest.get(".", ""))
    if released != _LAST_RELEASED:
        return (
            f"these pins described the pending 0.3.0 notes; the manifest is "
            f"now at {released}, so that release has been cut and the pins "
            "retire with it"
        )
    if _feeding_range() is None:
        return (
            f"the v{_LAST_RELEASED} tag is not resolvable in this checkout "
            "-- the pins need the feeding range, not a shallow clone"
        )
    if _fork_point() is None:
        return (
            "no fork point with a main-line ref computable (tried "
            "origin/main, main) -- the branch-quality pins need the "
            "branch's real history against its base"
        )
    return None


_skip = _skip_reason()
pytestmark = pytest.mark.skipif(_skip is not None, reason=_skip or "")


def _note_texts(range_ref: str) -> list[str]:
    """The breaking-note texts release-please would surface from a range.

    Models the parser's shape, not raw footer lines: each commit (and
    each ``BEGIN_NESTED_COMMIT`` block inside one -- every block is its
    own virtual commit) surfaces at most one note -- the single footer
    when there is exactly one, else the ``type!:`` subject. A message
    stacking several footers cannot be relied on for any particular one
    of them, so it contributes nothing here.
    """
    log = _git("log", "--format=%H%x00%B%x01", range_ref)
    notes: list[str] = []
    for block in log.split("\x01"):
        if not block.strip():
            continue
        _, _, body = block.partition("\x00")
        messages = _NESTED_BLOCK.findall(body) or [body]
        for message in messages:
            lines = message.splitlines()
            subject = lines[0] if lines else ""
            footers = [line for line in lines if _FOOTER_MARKER.match(line)]
            if len(footers) == 1:
                notes.append(footers[0])
            elif _SUBJECT_MARKER.match(subject):
                notes.append(subject)
    return notes


def _touching(notes: list[str], *needles: str) -> list[str]:
    return [note for note in notes if any(needle in note for needle in needles)]


def _assert_heartbeat_entry_corrected(entry: str) -> None:
    """Passing a *value* is fine; only a non-positive one is rejected, and
    the parameter is now enforced by the reclaim sweep. An entry claiming
    the parameter itself "now raises ValueError" tells an adopter to rip
    out every ``heartbeat_timeout=`` call site when only zero-or-negative
    values need to change, and an entry still calling the value unread
    denies the enforcement that shipped."""
    assert "non-positive" in entry.lower() or "<= 0" in entry, (
        f"heartbeat_timeout entry does not scope the raise to a non-positive "
        f"value: {entry!r} -- current code (src/taskq/client/_args.py) "
        "rejects only a value <= 0; a healthy positive heartbeat_timeout "
        "is accepted exactly as before. The entry text feeds the generated "
        "release notes verbatim, so this wording ships to every 0.3.0 "
        "adopter reading the breaking-changes section"
    )
    assert "enforc" in entry.lower(), (
        f"heartbeat_timeout entry never says the parameter is now enforced: "
        f"{entry!r} -- the shipped change is that the reclaim sweep "
        "reclaims a holder silent past the timeout; an entry that omits "
        "enforcement still leaves the adopter with the ancestor history's "
        "'read by nothing' framing"
    )
    assert "read by nothing" not in entry and "never kept" not in entry, (
        f"heartbeat_timeout entry still describes the parameter as "
        f"unenforced: {entry!r} -- it is enforced by the leader's reclaim "
        "sweep (cause='heartbeat_timeout'); only the pre-0.3.0 behavior "
        "was store-and-ignore"
    )


def _assert_not_superseded_terminal_framing(entry: str) -> None:
    """No entry in the denial/max_attempts family may describe a
    denial-driven terminal failure -- the settled 429 semantics never let
    an admission denial by itself terminalize a job."""
    assert not ("fails terminally" in entry and "MaxAttemptsExceeded" in entry), (
        f"denial entry describes a denial-driven terminal failure: "
        f"{entry!r} -- the settled 429 semantics never let an admission "
        "denial by itself terminalize a job (see "
        "test_rate_limit_denial_docs_contract.py); an entry claiming "
        "otherwise ships the superseded behavior into the 0.3.0 notes "
        "verbatim"
    )


def _assert_denial_entry_settled(entry: str) -> None:
    """The settled admission-denial semantics are HTTP-429: a denial never
    by itself consumes retry budget or terminalizes a job -- the job is
    rescheduled until capacity frees or its ``schedule_to_close`` expires,
    and the ordinary deadline path fails it there. A superseded draft
    framing has the denial loop spending the job's retry budget to a
    terminal failure; release-please copies entry text unmodified, so
    that framing must not be what the range carries."""
    _assert_not_superseded_terminal_framing(entry)
    assert "schedule_to_close" in entry, (
        f"denial entry does not name schedule_to_close as the bound on a "
        f"never-admitted job: {entry!r} -- 429 semantics reschedule a "
        "denied job until capacity frees or its deadline expires through "
        "the ordinary deadline path; without the bound the entry "
        "misdescribes the only exit"
    )
    assert (
        "no retry budget" in entry
        or "consumes no" in entry
        or "never consumes" in entry
        or "without spending" in entry
    ), (
        f"denial entry never says a denial spends no retry budget: "
        f"{entry!r} -- that is the headline correction the notes must carry"
    )
    assert (
        "no per-denial" in entry
        or "no longer write" in entry
        or "no job_attempts" in entry
        or "no job_events" in entry
    ), (
        f"denial entry never says per-denial rows stopped: {entry!r} -- the "
        "counter columns (snooze_count, rate_limit_blocked_count) and the "
        "OTEL counters replace job_attempts/job_events rows, and the notes "
        "must point readers at them"
    )


def test_feeding_range_carries_corrected_heartbeat_timeout_entry() -> None:
    """The regenerated 0.3.0 notes must carry a corrected heartbeat entry.

    The immutable ancestor history (4a5da1e) carries the superseded
    outright-refusal framing, and its bullet will regenerate until the
    associated PR body overrides it or 0.3.0 ships; what these pins can
    and must guarantee is that a correctly-framed entry rides the range
    too, so the notes never carry the wrong story alone.
    """
    feeding = _feeding_range()
    assert feeding is not None  # the module-level skipif guarantees this
    entries = _touching(_note_texts(feeding), "heartbeat_timeout")
    assert entries, (
        "no commit in the release-please feeding range "
        f"({feeding}) surfaces a breaking note naming heartbeat_timeout "
        "-- release-please aggregates markers across the whole range when "
        "it builds the notes, so the corrected entry must ride one of the "
        "range's own commit messages; without it the generated notes can "
        "only repeat the superseded outright-refusal framing the immutable "
        "ancestor history already carries"
    )
    assert any("non-positive" in e.lower() or "<= 0" in e for e in entries), (
        "the feeding range carries heartbeat_timeout breaking entries but "
        "none with the corrected non-positive scoping -- the regenerated "
        "notes would keep telling adopters the parameter itself is refused"
    )


def test_feeding_range_carries_denial_accounting_and_dumps_entries() -> None:
    """The two breaking changes the generated notes never surfaced keep
    their markers.

    release-please only sees a breaking change if some commit in the
    range surfaces a note for it. These two entries are what stand
    between the snooze/denial accounting change and the ``dumps()``
    strictness change and total invisibility in the generated 0.3.0
    notes. (Both had footers on main already -- the denial one was
    dropped by the parser's one-note-per-commit limit, and the dumps one
    only survived by luck of paragraph ordering; neither can be relied
    on, so the range carries structurally unambiguous markers.)
    """
    feeding = _feeding_range()
    assert feeding is not None  # the module-level skipif guarantees this
    notes = _note_texts(feeding)
    assert any("dumps" in note for note in notes), (
        "no commit in the feeding range surfaces a breaking note for "
        "taskq._json.dumps() dropping OPT_NON_STR_KEYS -- otherwise "
        "release-please has no marker for it and the 0.3.0 notes ship with "
        "no mention of a change that turns previously-coerced int dict "
        "keys into a raised TypeError"
    )
    denial_entries = _touching(notes, *_DENIAL_NEEDLES)
    assert denial_entries, (
        "no commit in the feeding range surfaces a breaking note for the "
        "snooze/denial accounting change -- otherwise release-please has "
        "no marker for it and the 0.3.0 notes never mention that "
        "per-denial job_attempts/job_events rows stopped being written"
    )
    assert any(
        "schedule_to_close" in entry and ("consumes no retry budget" in entry)
        for entry in denial_entries
    ), (
        "the feeding range's denial entries never state the settled 429 "
        "semantics (bounded by schedule_to_close, consuming no retry "
        "budget) -- the regenerated notes would carry the superseded "
        "budget-consumption framing"
    )


def test_branch_notes_are_framed_correctly() -> None:
    """Every breaking note this branch adds is framed the way the shipped
    behavior reads.

    The feeding range necessarily contains the immutable ancestor's
    superseded footers; this branch's own commits are the mutable part,
    and every note they add -- corrected, re-marked or brand new -- is
    held to the full contract: a heartbeat entry must scope the raise to
    non-positive values and state the enforcement, and a denial-family
    entry must state the 429 semantics, never the reversed draft.
    """
    fork_point = _fork_point()
    assert fork_point is not None  # the module-level skipif guarantees this
    notes = _note_texts(f"{fork_point}..HEAD")
    for entry in _touching(notes, "heartbeat_timeout"):
        _assert_heartbeat_entry_corrected(entry)
    # The wide family (anything touching max_attempts or the row tables)
    # is held to the negative contract only -- retry_job's ceiling raise
    # legitimately touches max_attempts without being a denial entry. The
    # full 429 contract applies to the entries that actually describe
    # denials or snoozes.
    for entry in _touching(notes, *_DENIAL_NEEDLES):
        _assert_not_superseded_terminal_framing(entry)
    for entry in _touching(notes, "denial", "snooze"):
        _assert_denial_entry_settled(entry)


def test_upgrading_guide_heartbeat_timeout_section_matches_enqueue_boundary_check() -> None:
    """upgrading.md's own heartbeat_timeout section must describe the real guard.

    This is the text operators actually read for migration guidance (the
    generated release notes point readers here). It must say the reject
    condition is a non-positive value, not that supplying the parameter
    itself is now refused.
    """
    text = _UPGRADING.read_text()
    start = text.index("### `heartbeat_timeout` is enforced by the reclaim sweep")
    section = text[start:].split("\n### ", 1)[0]
    assert "now raises `ValueError`" in section, (
        "upgrading.md must still document that a bad heartbeat_timeout "
        "raises ValueError at the enqueue boundary"
    )
    assert "non-positive value now raises" in section or "zero-or-negative" in section, (
        "upgrading.md's heartbeat_timeout section must scope the raise to "
        "non-positive values -- a healthy positive heartbeat_timeout is "
        "accepted and enforced, not refused"
    )
