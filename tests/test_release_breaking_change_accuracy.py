"""The generated 0.3.0 release notes must describe what actually shipped.

release-please writes the ``⚠ BREAKING CHANGES`` section of a release PR
purely from conventional-commit markers (``type!:`` subjects or
``BREAKING CHANGE:`` footers) on the commits it is releasing -- never from
hand-written prose. Two independent ways that pipeline can lie to an
operator reading the 0.3.0 notes:

1. A marked commit's own footer text drifts from the behavior that
   actually ships, so even a "correctly captured" marker produces a wrong
   bullet.
2. A real breaking change ships on a commit with no marker at all, so
   release-please never sees it and the notes simply omit it.

``4a5da1e`` (on ``main``) is both failure modes at once. It carries three
``BREAKING CHANGE:`` footers -- heartbeat_timeout enforcement, denial
accounting, and ``taskq._json.dumps()`` strictness -- but the generated
0.3.0 release PR (#36) surfaced only the first, and that footer's own
wording ("passing heartbeat_timeout to any enqueue API ... now raises
ValueError") is wrong on current code: ``_args.py`` only rejects a
non-positive *value*, not the parameter itself (mirrored correctly in
``docs/guides/upgrading.md``). The other two footers -- and the several
other unmarked breaking commits behind PR #120 and PR #148 -- never reached
the notes because release-please only reads markers, and these tests pin
that the source-of-truth commit's marker text matches shipped behavior so
a future regeneration of the notes is trustworthy once the marker
propagation gap is closed.

These tests read git history and doc prose -- never source structure --
so they hold across any refactor that leaves the documented behavior and
commit trailer intact.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_UPGRADING = _REPO_ROOT / "docs" / "guides" / "upgrading.md"

# The commit release-please would read the breaking-change footers from --
# reachable on `main`, so the pin holds regardless of which branch runs it.
_MARKER_COMMIT = "4a5da1e"


def _git_show(*args: str) -> str:
    result = subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
        check=True,
    )
    return result.stdout


def _commit_reachable_from_head(sha: str) -> bool:
    # The commit ships to main (and from there into a release-please PR)
    # through this branch's own history, not main's current tip -- HEAD is
    # the correct ancestor check for "will feed the next release notes".
    result = subprocess.run(  # noqa: S603
        ["git", "merge-base", "--is-ancestor", sha, "HEAD"],  # noqa: S607
        cwd=_REPO_ROOT,
        capture_output=True,
    )
    return result.returncode == 0


pytestmark = pytest.mark.skipif(
    not _commit_reachable_from_head(_MARKER_COMMIT),
    reason=(
        f"{_MARKER_COMMIT} is not reachable from HEAD in this checkout -- "
        "the pin needs the real release history, not a shallow or "
        "history-rewritten clone"
    ),
)


def test_marker_commit_footer_matches_shipped_heartbeat_timeout_behavior() -> None:
    """The heartbeat_timeout BREAKING CHANGE footer must not overstate the guard.

    Passing a *value* is fine; only a non-positive one is rejected. A
    footer claiming the parameter itself "now raises ValueError" tells an
    adopter to rip out every heartbeat_timeout= call site, when only
    zero-or-negative values need to change.
    """
    body = _git_show("log", "-1", "--format=%B", _MARKER_COMMIT)
    footers = [
        line
        for line in body.splitlines()
        if line.startswith("BREAKING CHANGE:") and "heartbeat_timeout" in line
    ]
    assert footers, (
        f"expected a heartbeat_timeout BREAKING CHANGE footer on "
        f"{_MARKER_COMMIT} -- if it was removed, the notes lose the "
        "marker that makes release-please surface this change at all"
    )
    footer = footers[0]
    assert "now raises ValueError" not in footer or "non-positive" in footer.lower(), (
        f"the footer says passing heartbeat_timeout itself now raises "
        f"ValueError: {footer!r} -- current code (src/taskq/client/_args.py) "
        "only rejects a value <= 0; a healthy positive heartbeat_timeout is "
        "accepted exactly as before. The footer text feeds the generated "
        "release notes verbatim, so this wording ships to every 0.3.0 "
        "adopter reading the breaking-changes section"
    )


def test_marker_commit_carries_denial_accounting_and_dumps_footers() -> None:
    """The other two real breaking changes on the marker commit keep their footers.

    release-please only sees a breaking change if its commit carries a
    marker. These two footers are what stand between the snooze/denial
    accounting change and the dumps() strictness change and total
    invisibility in the generated 0.3.0 notes.
    """
    body = _git_show("log", "-1", "--format=%B", _MARKER_COMMIT)
    footer_lines = [line for line in body.splitlines() if line.startswith("BREAKING CHANGE:")]
    assert any("dumps" in line for line in footer_lines), (
        f"{_MARKER_COMMIT} must keep a BREAKING CHANGE footer for "
        "taskq._json.dumps() dropping OPT_NON_STR_KEYS -- otherwise "
        "release-please has no marker for it and the 0.3.0 notes ship "
        "with no mention of a change that turns previously-coerced int "
        "dict keys into a raised TypeError"
    )
    denial_footers = [
        line
        for line in footer_lines
        if "max_attempts" in line or "job_attempts" in line or "denial" in line.lower()
    ]
    assert denial_footers, (
        f"{_MARKER_COMMIT} must keep a BREAKING CHANGE footer for the "
        "snooze/denial accounting change -- otherwise release-please has "
        "no marker for it and the 0.3.0 notes never mention that "
        "per-denial job_attempts/job_events rows stopped being written"
    )


def test_marker_commit_denial_footer_does_not_encode_superseded_budget_semantics() -> None:
    """The denial-accounting footer must describe final 429 semantics, not the reversed draft.

    The spec settled admission denials as HTTP-429 semantics: a denial
    never by itself consumes retry budget or terminalizes a job, full
    stop -- it is rescheduled until capacity frees or schedule_to_close
    expires. An earlier draft of this same commit's footer describes the
    opposite: a transient job with no schedule_to_close failing
    terminally once its retry budget is spent from denials alone. That
    draft framing must not be what ships in the notes.
    """
    body = _git_show("log", "-1", "--format=%B", _MARKER_COMMIT)
    footer_lines = [line for line in body.splitlines() if line.startswith("BREAKING CHANGE:")]
    denial_footer = next(
        (
            line
            for line in footer_lines
            if "max_attempts" in line or "job_attempts" in line or "denial" in line.lower()
        ),
        "",
    )
    assert denial_footer, "expected a denial-accounting BREAKING CHANGE footer to exist"
    assert "fails terminally" not in denial_footer or "MaxAttemptsExceeded" not in denial_footer, (
        f"the denial footer describes a denial-driven terminal failure: "
        f"{denial_footer!r} -- the settled 429 semantics never let an "
        "admission denial by itself terminalize a job (see "
        "test_rate_limit_denial_docs_contract.py); a footer claiming "
        "otherwise ships the superseded behavior into the 0.3.0 notes "
        "verbatim, since release-please copies footer text unmodified"
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
