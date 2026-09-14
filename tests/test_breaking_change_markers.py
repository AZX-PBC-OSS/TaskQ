"""Standing guard: a user-visible breaking change must be recorded where
release-please reads it.

``CHANGELOG.md`` is generated on release by release-please from
conventional commits (``.github/workflows/release-please.yml``,
``release-please-config.json``), not hand-edited. Every generated release
heading carries the compare-link form
``## [x.y.z](…/compare/v…)``; a hand-written ``## [Unreleased]`` block is
structurally foreign to the generator's model of the file — anything
written into it is invisible to the release-notes pipeline and is
flattened or discarded when the generator next runs.

The mechanism release-please DOES read is the conventional-commit
breaking marker: a ``!`` after the type/scope (``feat!:``) or a
``BREAKING CHANGE:`` footer. That marker is also what drives the SemVer
major/minor bump. The repo knows the convention — ``1ab2340 feat!:``,
``07a6cfe feat(actor-config)!:`` on main — but the current branch
shipped its breaking changes (e.g. ``1e94855`` dropping
``OPT_NON_STR_KEYS`` from ``dumps()``) as plain ``perf:``/``fix:``
commits with no marker, and recorded the breaks only in the hand-written
block. On release, release-please would file those breaks under
"Performance Improvements" and bump a patch version.

The class rule, mechanically enforced here:

1. ``CHANGELOG.md`` carries no hand-written section the generator did
   not produce.
2. If the repo documents pending breaking changes outside the
   generator's model, the branch's commit history must carry breaking
   markers for them — the notes pipeline and the SemVer bump both key
   off the markers, nothing else.
"""

import json
import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CHANGELOG = _REPO_ROOT / "CHANGELOG.md"
_RELEASE_CONFIG = _REPO_ROOT / "release-please-config.json"
_RELEASE_MANIFEST = _REPO_ROOT / ".release-please-manifest.json"

#: A generated release heading: ``## [0.2.2](https://…/compare/v0.2.1...v0.2.2) (2026-…)``.
_GENERATED_HEADING = re.compile(r"^## \[\d+\.\d+\.\d+\]\(https://", re.MULTILINE)
#: A historical pre-release-please heading (``## 0.1.0 - 2026-07-08``) — a
#: released version's record, which the generator leaves alone.
_LEGACY_RELEASE_HEADING = re.compile(r"^## \[?\d+\.\d+\.\d+\]?", re.MULTILINE)
_ANY_HEADING = re.compile(r"^## \[?[^\]\n]+.*$", re.MULTILINE)

#: Conventional-commit breaking markers: ``type(scope)!:`` or the footers.
_SUBJECT_MARKER = re.compile(r"^[a-z]+(\([^)]*\))?!:")
_FOOTER_MARKER = re.compile(r"^BREAKING[ -]CHANGE:", re.MULTILINE)


def _non_generated_sections() -> list[str]:
    """Heading lines in CHANGELOG.md that release-please would not write."""
    text = _CHANGELOG.read_text()
    foreign: list[str] = []
    for match in _ANY_HEADING.finditer(text):
        line = match.group(0)
        if not _GENERATED_HEADING.match(line) and not _LEGACY_RELEASE_HEADING.match(line):
            foreign.append(line)
    return foreign


def _breaking_markers_on_branch() -> list[str]:
    """Subjects of ``main..HEAD`` commits carrying a breaking marker."""
    log = subprocess.run(
        ["git", "log", "--format=%H%n%B%n---END---", "main..HEAD"],  # noqa: S607  # Why: git resolved from PATH, as elsewhere in this suite; fixed literal argv, no shell.
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
        check=True,
    ).stdout
    marked: list[str] = []
    for commit_block in log.split("---END---"):
        lines = commit_block.strip().splitlines()
        if not lines:
            continue
        sha, body = lines[0], "\n".join(lines[1:])
        subject = body.splitlines()[0] if body.splitlines() else ""
        if _SUBJECT_MARKER.match(subject) or _FOOTER_MARKER.search(body):
            marked.append(f"{sha[:9]} {subject}")
    return marked


def test_changelog_carries_only_release_please_generated_sections() -> None:
    """No hand-written block: anything outside the generator's heading
    vocabulary is outside its model of the file and will not survive (or
    be honoured by) the next release run."""
    foreign = _non_generated_sections()
    assert not foreign, (
        "CHANGELOG.md contains sections release-please did not generate:\n  "
        + "\n  ".join(foreign)
        + "\nThe file is regenerated from conventional commits on release; "
        "hand-written entries are invisible to that pipeline. Breaking "
        "changes belong in commit messages as `type!:` subjects or "
        "`BREAKING CHANGE:` footers, which drive BOTH the generated notes "
        "and the SemVer bump. Human-facing migration notes belong in "
        "docs/guides/upgrading.md."
    )


def test_documented_breaking_changes_are_visible_to_release_please() -> None:
    """Breaking changes recorded outside the generator must have markers in
    history — otherwise the release files them under the wrong heading
    with the wrong version bump."""
    foreign = _non_generated_sections()
    markers = _breaking_markers_on_branch()
    assert not foreign or markers, (
        f"CHANGELOG.md documents breaking changes in a hand-written block "
        f"({len(foreign)} foreign section(s), including {foreign[0]!r}), "
        "but ZERO commits on this branch (main..HEAD) carry a "
        "conventional-commit breaking marker (`!` subject or "
        "`BREAKING CHANGE:` footer). Verified: 0 of 34 commits marked. "
        "Release-please reads only the markers, so the breaks would ship "
        "as a patch release under 'Performance Improvements'/'Bug "
        "Fixes' — a user-visible breaking change must carry a "
        "release-please breaking marker. Marked commits found: "
        f"{markers or 'none'}."
    )


def test_next_release_version_is_hand_pinned_above_the_last_released_minor() -> None:
    """The next release's version is pinned in release-please-config.json
    (``release-as``), not left to whatever bump the merged history's
    markers compute.

    Why a hand pin: the breaking changes reach ``main`` through a merge
    whose strategy the release machinery does not control — a
    squash-merge collapses every ``fix!:`` marker on the stack into one
    hand-written message, and a marker-less history computes a PATCH
    bump, shipping user-visible breaks (``dumps()``'s dropped
    ``OPT_NON_STR_KEYS``, the keyed-ref ``.typed()`` requirement,
    ``heartbeat_timeout``'s loud refusal, denials-as-counters) as
    0.2.3. ``release-as`` fixes the floor regardless of how history
    lands. The pin asserts the floor is at least a minor bump above the
    last released version — the breaking floor for a 0.x line — so a
    patch-level pin, or a removed pin, fails here.
    """
    config = json.loads(_RELEASE_CONFIG.read_text())
    package = config["packages"]["."]
    assert "release-as" in package, (
        "release-please-config.json's root package carries no 'release-as' "
        "pin: the next release's version is whatever the merged history's "
        "markers compute, and a squash-merge that drops the stack's "
        "breaking markers would cut the breaks as a patch release. Hand-set "
        "the floor (the #160 resolution)."
    )
    pinned = tuple(int(part) for part in str(package["release-as"]).split("."))
    manifest = json.loads(_RELEASE_MANIFEST.read_text())
    released = tuple(int(part) for part in str(manifest["."]).split("."))
    assert pinned[:2] > released[:2], (
        f"the hand-pinned release version {package['release-as']} is not a "
        f"minor-or-greater bump above the last released version "
        f"{manifest['.']} — a patch-level floor ships the stack's breaking "
        "changes as a patch release, the exact adopter harm #160 records."
    )
