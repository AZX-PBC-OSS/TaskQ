"""The 0.3.0 breaking-change entries must be correct where release-please reads them.

release-please writes the ``⚠ BREAKING CHANGES`` section of a release PR
purely from conventional-commit markers (``type!:`` subjects or
``BREAKING CHANGE:`` footers) found across the whole commit range being
released -- never from hand-written prose and never from one nominated
commit. The truthfulness of the notes is therefore a property of the
range ``<base>..HEAD``, and these pins assert over exactly that range.
The base is the fork point -- the newest commit HEAD shares with any
main-line ref (``origin/main`` or a local ``main``) -- never the raw
ref: a stale local ``main`` points behind the real fork, and a range
taken from it pulls the immutable, already-published ancestor footers
into scope, making the pins fail on text this branch cannot change.
(The standing marker guard in ``test_breaking_change_markers.py`` can
afford the raw ref because its assertion is conditional on the range
being non-empty; these content assertions cannot.)

Why the range and not the ancestor commit that first carried these
footers: that ancestor is published on ``main``, so its message is
immutable shared history, and two of its footers encode framings that
were corrected before 0.3.0 ever shipped --

* its heartbeat_timeout entry claims that passing the parameter itself
  "now raises ValueError" and that the value was "read by nothing".
  Shipped behavior: the parameter is accepted and *enforced* (the
  leader's reclaim sweep reclaims a holder silent past it), and only a
  non-positive *value* raises ``ValueError`` at the enqueue boundary
  (mirrored correctly in ``docs/guides/upgrading.md``).
* its denial-accounting entry claims a denial loop spends the job's
  retry budget up to a terminal ``MaxAttemptsExceeded``. Shipped behavior
  (HTTP-429 semantics, pinned by ``test_rate_limit_denial_docs_contract.py``):
  a denial consumes no retry budget, writes no per-denial rows, and never
  by itself terminalizes a job -- the only bound on a never-admitted job
  is its ``schedule_to_close`` deadline.

Amending published history is off the table; the honest mechanism is the
one release-please actually implements: this branch's own commits carry
the corrected entries as fresh markers, and aggregation surfaces them in
the regenerated notes alongside (and correcting) the stale ones. These
pins therefore fail while no commit in the range carries the corrected
markers; they go green once the branch's release commit lands with the
footer text these assertions spell out -- which is also the text the
generated 0.3.0 release notes are corrected to match.

The third required entry is ``taskq._json.dumps()`` dropping
``OPT_NON_STR_KEYS`` (non-str dict keys now raise ``TypeError``) -- a real
breaking change whose marker never reached the generated notes.

These tests read git history and doc prose -- never source structure --
so they hold across any refactor that leaves the documented behavior and
the range's marker text intact.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_UPGRADING = _REPO_ROOT / "docs" / "guides" / "upgrading.md"

#: Conventional-commit breaking markers: ``type(scope)!:`` subjects, or the footers.
_SUBJECT_MARKER = re.compile(r"^[a-z]+(\([^)]*\))?!:")
_FOOTER_MARKER = re.compile(r"^BREAKING[ -]CHANGE:")

#: Main-line ref candidates: a CI PR checkout (detached head) carries only
#: the remote-tracking ``origin/main``; a local checkout usually has both.
_MAIN_LINE_CANDIDATES = ("origin/main", "main")


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


_FORK_POINT = _fork_point()

pytestmark = pytest.mark.skipif(
    _FORK_POINT is None,
    reason=(
        "no fork point with a main-line ref computable (tried origin/main, "
        "main) -- the pin needs the branch's real history against its base, "
        "not a shallow or history-rewritten clone"
    ),
)

#: Needles that identify the snooze/denial-accounting entry among the range's
#: breaking markers.
_DENIAL_NEEDLES = ("denial", "snooze", "job_attempts", "job_events", "max_attempts")


def _breaking_entries_on_branch() -> list[tuple[str, str]]:
    """Every breaking entry release-please would aggregate from the range.

    Returns ``(sha, entry)`` pairs: each commit's ``BREAKING CHANGE:``
    footer lines, plus its subject line when the subject carries the ``!``
    marker (release-please surfaces the subject as the entry for that
    form). Entry text is what the generated notes copy verbatim.
    """
    assert _FORK_POINT is not None  # the module-level skipif guarantees this
    log = _git("log", "--format=%H%n%B%n---END---", f"{_FORK_POINT}..HEAD")
    entries: list[tuple[str, str]] = []
    for block in log.split("---END---"):
        lines = block.strip().splitlines()
        if not lines:
            continue
        sha, body = lines[0][:9], "\n".join(lines[1:])
        subject = body.splitlines()[0] if body.splitlines() else ""
        if _SUBJECT_MARKER.match(subject):
            entries.append((sha, subject))
        entries.extend((sha, line) for line in body.splitlines() if _FOOTER_MARKER.match(line))
    return entries


def _entries_touching(entries: list[tuple[str, str]], *needles: str) -> list[tuple[str, str]]:
    return [(sha, text) for sha, text in entries if any(n in text for n in needles)]


def test_branch_range_carries_corrected_heartbeat_timeout_entry() -> None:
    """Every heartbeat_timeout breaking entry in the range must carry the corrected framing.

    Passing a *value* is fine; only a non-positive one is rejected, and the
    parameter is now enforced by the reclaim sweep. An entry claiming the
    parameter itself "now raises ValueError" tells an adopter to rip out
    every ``heartbeat_timeout=`` call site when only zero-or-negative
    values need to change, and an entry still calling the value unread
    denies the enforcement that shipped.
    """
    entries = _entries_touching(_breaking_entries_on_branch(), "heartbeat_timeout")
    assert entries, (
        "no commit in this branch's range carries a breaking marker naming "
        "heartbeat_timeout -- release-please aggregates markers across the "
        "whole range when it builds the notes, so the corrected entry must "
        "ride one of this branch's own commit messages; without it the "
        "generated notes can only repeat the superseded outright-refusal "
        "framing the immutable ancestor history already carries"
    )
    for sha, entry in entries:
        assert "non-positive" in entry.lower() or "<= 0" in entry, (
            f"{sha}'s heartbeat_timeout entry does not scope the raise to a "
            f"non-positive value: {entry!r} -- current code "
            "(src/taskq/client/_args.py) rejects only a value <= 0; a "
            "healthy positive heartbeat_timeout is accepted exactly as "
            "before. The entry text feeds the generated release notes "
            "verbatim, so this wording ships to every 0.3.0 adopter reading "
            "the breaking-changes section"
        )
        assert "enforc" in entry.lower(), (
            f"{sha}'s heartbeat_timeout entry never says the parameter is "
            f"now enforced: {entry!r} -- the shipped change is that the "
            "reclaim sweep reclaims a holder silent past the timeout; an "
            "entry that omits enforcement still leaves the adopter with "
            "the ancestor history's 'read by nothing' framing"
        )
        assert "read by nothing" not in entry and "never kept" not in entry, (
            f"{sha}'s heartbeat_timeout entry still describes the parameter "
            f"as unenforced: {entry!r} -- it is enforced by the leader's "
            "reclaim sweep (cause='heartbeat_timeout'); only the pre-0.3.0 "
            "behavior was store-and-ignore"
        )


def test_branch_range_carries_denial_accounting_and_dumps_entries() -> None:
    """The two breaking changes the generated notes never surfaced keep their markers.

    release-please only sees a breaking change if some commit in the range
    carries a marker. These two entries are what stand between the
    snooze/denial accounting change and the ``dumps()`` strictness change
    and total invisibility in the generated 0.3.0 notes.
    """
    entries = _breaking_entries_on_branch()
    assert any("dumps" in text for _, text in entries), (
        "no commit in this branch's range carries a breaking marker for "
        "taskq._json.dumps() dropping OPT_NON_STR_KEYS -- otherwise "
        "release-please has no marker for it and the 0.3.0 notes ship with "
        "no mention of a change that turns previously-coerced int dict "
        "keys into a raised TypeError"
    )
    assert _entries_touching(entries, *_DENIAL_NEEDLES), (
        "no commit in this branch's range carries a breaking marker for the "
        "snooze/denial accounting change -- otherwise release-please has no "
        "marker for it and the 0.3.0 notes never mention that per-denial "
        "job_attempts/job_events rows stopped being written"
    )


def test_denial_entry_describes_429_semantics_not_superseded_budget_consumption() -> None:
    """The denial-accounting entry must describe final 429 semantics, not the reversed draft.

    The spec settled admission denials as HTTP-429 semantics: a denial
    never by itself consumes retry budget or terminalizes a job -- the job
    is rescheduled until capacity frees or its ``schedule_to_close``
    expires, and the ordinary deadline path fails it there. A superseded
    draft framing has the denial loop spending the job's retry budget to a
    terminal failure; release-please copies entry text unmodified, so that
    framing must not be what the range carries.
    """
    entries = _breaking_entries_on_branch()
    denial_entries = _entries_touching(entries, *_DENIAL_NEEDLES)
    assert denial_entries, "expected a denial-accounting breaking entry in the branch's range"
    for sha, entry in denial_entries:
        assert "fails terminally" not in entry or "MaxAttemptsExceeded" not in entry, (
            f"{sha}'s denial entry describes a denial-driven terminal "
            f"failure: {entry!r} -- the settled 429 semantics never let an "
            "admission denial by itself terminalize a job (see "
            "test_rate_limit_denial_docs_contract.py); an entry claiming "
            "otherwise ships the superseded behavior into the 0.3.0 notes "
            "verbatim"
        )
    semantic_entries = _entries_touching(entries, "denial", "snooze")
    for sha, entry in semantic_entries:
        assert "schedule_to_close" in entry, (
            f"{sha}'s denial entry does not name schedule_to_close as the "
            f"bound on a never-admitted job: {entry!r} -- 429 semantics "
            "reschedule a denied job until capacity frees or its deadline "
            "expires through the ordinary deadline path; without the bound "
            "the entry misdescribes the only exit"
        )
        assert (
            "no retry budget" in entry
            or "consumes no" in entry
            or "never consumes" in entry
            or "without spending" in entry
        ), (
            f"{sha}'s denial entry never says a denial spends no retry "
            f"budget: {entry!r} -- that is the headline correction the "
            "notes must carry"
        )
        assert (
            "no per-denial" in entry
            or "no longer write" in entry
            or "no job_attempts" in entry
            or "no job_events" in entry
        ), (
            f"{sha}'s denial entry never says per-denial rows stopped: "
            f"{entry!r} -- the counter columns (snooze_count, "
            "rate_limit_blocked_count) and the OTEL counters replace "
            "job_attempts/job_events rows, and the notes must point "
            "readers at them"
        )


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
