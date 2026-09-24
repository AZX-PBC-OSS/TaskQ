"""Standing guard: a user-visible breaking change must be recorded where
release-please reads it, and what it reads is narrower than the repo.

``CHANGELOG.md`` is generated on release by release-please from
conventional commits (``.github/workflows/release-please.yml``,
``release-please-config.json``), not hand-edited. Every generated release
heading carries the compare-link form
``## [x.y.z](…/compare/v…)``; a hand-written ``## [Unreleased]`` block is
structurally foreign to the generator's model of the file - anything
written into it is invisible to the release-notes pipeline and is
flattened or discarded when the generator next runs.

The mechanism release-please DOES read is the conventional-commit
breaking marker: a ``!`` after the type/scope (``feat!:``) or a
``BREAKING CHANGE:`` footer. Those markers drive BOTH the generated
notes' breaking section and the SemVer bump. Two properties of the
mechanism, both learned from the 0.3.0 drift this guard was rebuilt
around, shape what counts as compliant:

1. **One note per commit.** The parser surfaces at most ONE breaking
   note per commit message: a message stacking several ``BREAKING CHANGE:``
   paragraphs silently drops all but one (which one survives depends on
   the paragraph layout; run-together footer lines keep only the last).
   A marker only counts here when it is structurally unambiguous: a
   ``type!:`` subject, a single-footer message, or one footer inside a
   ``BEGIN_NESTED_COMMIT``/``END_NESTED_COMMIT`` block (each block becomes
   its own virtual commit, which is how one commit carries many markers).
2. **The walk is windowed.** release-please collects commits by walking
   the target branch's newest history looking for the last release SHA
   and stops after ``commit-search-depth`` commits (default 500) whether
   or not it found it; the notes are built from that window, not
   automatically from ``<last-tag>..HEAD``. When a release cycle outgrows
   the configured depth, real breaking markers fall out of the window
   without any error: five markers were silently lost that way during
   the 0.3.0 cycle (a3013fb, 07a6cfe, abc38d1, 12fd571, 801095c, the
   last a correctly-marked ``feat(testing)!`` break with no
   upgrading.md section at all).

The class rules, mechanically enforced here:

1. ``CHANGELOG.md`` carries no hand-written section the generator did
   not produce.
2. Every breaking change the docs promise (a breaking-flagged section of
   ``docs/guides/upgrading.md``, plus the explicit policy entries below)
   is recorded where release-please reads it, and each policy entry
   carries the release whose notes must carry it, so the census is
   critical at every point of the entry's life: while that release
   is pending, the needle must hit a carrier inside the range the walk
   covers; once the manifest reaches that release (the cut, where the release
   PR bumps the manifest and merges in the generated CHANGELOG), the
   needle is verified against the shipped CHANGELOG's breaking bullets
   for that version instead. The census therefore retires by
   construction at the cut rather than going permanently red against a
   range the carriers no longer sit in, and stays critical after
   it, because a regeneration that drops a shipped entry fails here.
3. The walk's configured depth covers the whole ``<last-shipped-tag>..HEAD``
   range, so the markers the docs promise are inside the window. The
   anchor is the last SHIPPED release: the manifest's version when its
   tag exists, else the newest resolvable version tag below it (the
   release-PR branch bumps the manifest ahead of the tag; release-please
   itself anchors at the last shipped release in that state).
4. The ``release-as`` pin, when present, is a floor for a pending
   release, never a stale leftover pinning an already-shipped version.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CHANGELOG = _REPO_ROOT / "CHANGELOG.md"
_UPGRADING = _REPO_ROOT / "docs" / "guides" / "upgrading.md"
_RELEASE_CONFIG = _REPO_ROOT / "release-please-config.json"
_RELEASE_MANIFEST = _REPO_ROOT / ".release-please-manifest.json"

#: A generated release heading: ``## [0.2.2](https://…/compare/v0.2.1...v0.2.2) (2026-…)``.
_GENERATED_HEADING = re.compile(r"^## \[\d+\.\d+\.\d+\]\(https://", re.MULTILINE)
#: A historical pre-release-please heading (``## 0.1.0 - 2026-07-08``) - a
#: released version's record, which the generator leaves alone.
_LEGACY_RELEASE_HEADING = re.compile(r"^## \[?\d+\.\d+\.\d+\]?", re.MULTILINE)
_ANY_HEADING = re.compile(r"^## \[?[^\]\n]+.*$", re.MULTILINE)

#: Conventional-commit breaking markers: ``type(scope)!:`` or the footers.
_SUBJECT_MARKER = re.compile(r"^[a-z]+(\([^)]*\))?!:")
_FOOTER_MARKER = re.compile(r"^BREAKING[ -]CHANGE:", re.MULTILINE)
_NESTED_BLOCK = re.compile(r"BEGIN_NESTED_COMMIT\n(.*?)\nEND_NESTED_COMMIT", re.DOTALL)

#: A version tag this repo's release machinery can produce: ``v0.2.2``.
_VERSION_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")

#: release-please's default walk depth when the config does not set one
#: (``manifest.ts``: ``DEFAULT_COMMIT_SEARCH_DEPTH = 500``).
_DEFAULT_COMMIT_SEARCH_DEPTH = 500


def _git(*args: str) -> str:
    result = subprocess.run(  # noqa: S603  # Why: fixed literal argv shape: git from PATH, as elsewhere in this suite; the range's base is this file's own tag derivation, no shell.
        ["git", *args],  # noqa: S607  # Why: git resolved from PATH, as elsewhere in this suite; fixed literal argv, no shell.
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
        check=True,
    )
    return result.stdout


def _version_tuple(text: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", text.strip())
    assert match is not None, f"not a x.y.z version: {text!r}"
    return (int(match[1]), int(match[2]), int(match[3]))


def _released_version() -> tuple[int, int, int]:
    """The manifest's last-released version (the root package's)."""
    manifest = json.loads(_RELEASE_MANIFEST.read_text())
    return _version_tuple(str(manifest["."]))


def _last_shipped_tag() -> str:
    """The tag of the last SHIPPED release: release-please's own anchor.

    Normally that is ``v<manifest>``. On the release PR branch the
    manifest is bumped to the release being cut *before* its tag exists;
    release-please anchors at the last shipped release in that state, and
    so does this guard: the newest resolvable version tag at or below the
    manifest's version. Loud when nothing qualifies: this guard's
    assertions are about the range the walk covers, and a checkout that
    cannot see any release tag cannot answer them.
    """
    released = _released_version()
    tags = [
        line.strip()
        for line in _git("tag", "--list").splitlines()
        if _VERSION_TAG.match(line.strip())
    ]
    resolvable = [_version_tuple(tag) for tag in tags if _version_tuple(tag) <= released]
    if not resolvable:
        raise RuntimeError(
            "no resolvable version tag at or below the manifest's "
            f"{released}: the breaking-change guard needs the range "
            "release-please walks, and a checkout without release tags "
            "cannot answer it (full-history fetch, per the CI checkouts "
            "this suite runs in)"
        )
    return f"v{'.'.join(str(part) for part in max(resolvable))}"


def _release_range() -> str:
    """The commit range the release walk covers: ``<last-shipped>..HEAD``."""
    return f"{_last_shipped_tag()}..HEAD"


def _carriers_in_range() -> list[tuple[str, str]]:
    """Breaking markers in the release range that survive the parser.

    Returns ``(sha, text)`` pairs where ``text`` is the note text
    release-please would surface verbatim. Only structurally unambiguous
    markers count: the parser yields at most one note per commit
    message, so a bare message stacking several ``BREAKING CHANGE:``
    footers cannot be relied on to surface any particular one of them
    (the 0.3.0 notes lost the snooze/denial marker exactly this way).
    Compliant forms:

    * a ``type!:`` subject (the subject itself becomes the note);
    * a bare message with exactly one ``BREAKING CHANGE:`` footer;
    * one ``BREAKING CHANGE:`` footer inside a nested-commit block
      (each block is its own virtual commit, so each note is
      independent).
    """
    log = _git("log", "--format=%H%x00%B%x01", _release_range())
    carriers: list[tuple[str, str]] = []
    for block in log.split("\x01"):
        if not block.strip():
            continue
        sha, _, body = block.partition("\x00")
        subject = body.splitlines()[0] if body.splitlines() else ""
        nested_blocks = _NESTED_BLOCK.findall(body)
        if nested_blocks:
            for nested in nested_blocks:
                nested_subject = nested.splitlines()[0] if nested.splitlines() else ""
                nested_footers = [
                    line for line in nested.splitlines() if _FOOTER_MARKER.match(line)
                ]
                if _SUBJECT_MARKER.match(nested_subject):
                    carriers.append((sha, nested_subject))
                if len(nested_footers) == 1:
                    carriers.append((sha, nested_footers[0]))
            continue
        footers = [line for line in body.splitlines() if _FOOTER_MARKER.match(line)]
        if _SUBJECT_MARKER.match(subject):
            carriers.append((sha, subject))
        if len(footers) == 1:
            carriers.append((sha, footers[0]))
    return carriers


def _non_generated_sections() -> list[str]:
    """Heading lines in CHANGELOG.md that release-please would not write."""
    text = _CHANGELOG.read_text()
    foreign: list[str] = []
    for match in _ANY_HEADING.finditer(text):
        line = match.group(0)
        if not _GENERATED_HEADING.match(line) and not _LEGACY_RELEASE_HEADING.match(line):
            foreign.append(line)
    return foreign


def _shipped_breaking_bullets(version: str) -> list[str] | None:
    """The breaking bullets of a SHIPPED release's CHANGELOG section.

    ``None`` when CHANGELOG.md has no section for that version; the
    bullet texts (minus the leading ``* ``) otherwise, the same note
    texts the generated release notes carry, so a needle that hits a
    carrier hits the shipped bullet for the same entry.
    """
    text = _CHANGELOG.read_text()
    heading = re.search(rf"^## \[{re.escape(version)}\]\(https://", text, re.MULTILINE)
    if heading is None:
        return None
    section = text[heading.end() :].split("\n## [", 1)[0]
    breaking = re.search(r"^### .*BREAKING", section, re.MULTILINE)
    if breaking is None:
        return []
    bullets: list[str] = []
    for line in section[breaking.end() :].splitlines():
        if line.startswith("### "):
            break
        if line.startswith("* "):
            bullets.append(line[2:])
    return bullets


#: The policy map: every section of docs/guides/upgrading.md that
#: promises a breaking change, keyed by a fragment of its heading and
#: carrying ``(target_release, needles)``: the release whose notes must
#: carry the entry, and the needle a carrier (while pending) or a
#: shipped CHANGELOG bullet (once cut) must contain. The completeness half
#: of the guard (see ``_breaking_flagged_sections``) fails when a new
#: breaking-flagged section appears without a mapping entry, so the map
#: and the guide cannot drift apart silently; a future release's entries
#: carry that release as their target and go pending until it cuts.
#:
#: Entries whose flag lives in the section prose rather than a
#: "Breaking" blockquote (the table sections) are policy additions the
#: census established; they are held to the same contract.
_BREAKING_SECTION_ENTRIES: dict[str, tuple[str, tuple[str, ...]]] = {
    "`taskq.worker.actor_config` → `taskq.actor_config`": (
        "0.3.0",
        ("taskq.worker.actor_config",),
    ),
    "`taskq.worker.actor_config_ops` → `taskq.actor_config_ops`": (
        "0.3.0",
        ("taskq.worker.actor_config_ops",),
    ),
    "`validate_actor_payload`: `actor_name=` → `actor=`": (
        "0.3.0",
        ("actor_name",),
    ),
    "Keyed refs: `payload_type` is required, `key_fn` receives the model": (
        "0.3.0",
        ("payload_type",),
    ),
    "Dispatch-path malformed payloads fail immediately": (
        "0.3.0",
        ("validate payload",),
    ),
    '`wait_for_batch` defaults to `on_empty="error"`': (
        "0.3.0",
        ("on_empty",),
    ),
    "Sub-enqueue failure events carry `error_class`/`error_message`": (
        "0.3.0",
        ("sub_enqueue_re_enqueue_error",),
    ),
    "dotenvmodel 1.x: environment-variable precedence flips": (
        "0.3.0",
        ("dotenvmodel",),
    ),
    "Time is unified on the database clock": (
        "0.3.0",
        # The rendered note is the footer text, not the bang subject:
        # the needle must hit what actually ships in the bullets.
        ("EnqueueArgs.scheduled_at",),
    ),
    "`firstof`/`allof` DST strategies become live": (
        "0.3.0",
        ("dst_strategy",),
    ),
    "`worker_id` is no longer a metric dimension": (
        "0.3.0",
        ("metric dimension",),
    ),
    "`taskq._json.dumps()` requires `str` dict keys": (
        "0.3.0",
        ("str dict keys",),
    ),
    "`heartbeat_timeout` is enforced by the reclaim sweep": (
        "0.3.0",
        ("heartbeat_timeout",),
    ),
    "Reservation and rate-limit denials are counters, not event rows": (
        "0.3.0",
        ("rate_limit_blocked_count",),
    ),
    "Snoozing and denials no longer raise `max_attempts`; both refund the claim's attempt": (
        "0.3.0",
        ("no longer raise jobs.max_attempts",),
    ),
    "`unique_for`'s default `unique_states` now includes `succeeded`": (
        "0.3.0",
        ("unique_states",),
    ),
    "A cross-actor `idempotency_key` hit now raises": (
        "0.3.0",
        ("IdempotencyKeyActorMismatchError",),
    ),
    "Bulk enqueues partition `max_pending` admission per actor": (
        "0.3.0",
        ("BatchMaxPendingExceededError",),
    ),
    "`taskq.cron.consecutive_failures` is relabeled and bounded": (
        "0.3.0",
        ("consecutive_failures",),
    ),
    "Structured-log event rename: `state_change` → `state-change`": (
        "0.3.0",
        ("state-change",),
    ),
    "Trailing newlines in queue names, tags, and keyed keys": (
        "0.3.0",
        ("trailing newline",),
    ),
    "Bounded inputs": (
        "0.3.0",
        ("BatchFilter",),
    ),
    "Configuration that no longer loads": (
        "0.3.0",
        ("WorkerSettings",),
    ),
    "`@actor(...)` capacity literals no longer win over the stored row": (
        "0.3.0",
        ("capacity fields",),
    ),
    "Rate-limit refunds now credit the store that paid": (
        "0.3.0",
        ("refund the store",),
    ),
    "The SAML cookie-less ACS fallback is opt-in and defaults off": (
        "0.3.0",
        ("allow_cookieless_fallback",),
    ),
    "`/logout` is now a POST with a session-bound CSRF token": (
        "0.3.0",
        ("GET /logout",),
    ),
    "Worker pools send no per-connection GUCs in the startup packet": (
        "0.3.0",
        ("startup packet",),
    ),
    "Queue and actor names are bounded at 255 characters": (
        "0.3.0",
        ("bounded at 255 characters",),
    ),
}


def _breaking_flagged_sections() -> list[str]:
    """Headings of upgrading.md sections flagged as breaking.

    A section flags itself with a blockquote whose text says "breaking"
    (the guide's convention: ``> **Unreleased.** Breaking for …``) and
    does not retract it ("not a breaking change"). Table sections whose
    break lives in the prose (``Bounded inputs``, ``Configuration that
    no longer loads``, trailing newlines) do not carry the flag; they
    are pinned by the policy map directly.
    """
    text = _UPGRADING.read_text()
    flagged: list[str] = []
    for part in re.split(r"^#{2,3} ", text, flags=re.MULTILINE)[1:]:
        lines = part.splitlines()
        heading = lines[0].strip()
        blockquote: list[str] = []
        for line in lines[1:]:
            if line.startswith(">"):
                blockquote.append(line.lstrip("> "))
            elif blockquote:
                break
        bq_text = " ".join(blockquote)
        if "breaking" in bq_text.lower() and "not a breaking change" not in bq_text.lower():
            flagged.append(heading)
    return flagged


def _mapped_section_key(heading: str) -> str | None:
    for key in _BREAKING_SECTION_ENTRIES:
        if key in heading:
            return key
    return None


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
    """Every breaking change the docs promise is recorded where the
    machinery reads it, at every point of the entry's life.

    While the entry's target release is pending (the manifest's released
    version is below it), the needle must hit a structurally unambiguous
    carrier in the walk range; otherwise the release files the break
    under the wrong heading with the wrong version bump, or omits it
    entirely. The markers are counted over ``<last-shipped-tag>..HEAD``
    (whose depth the next guard pins), and only in the forms the parser
    reliably surfaces. The 0.3.0 cycle lost real markers both ways:
    five to the walk window, and the snooze/denial accounting break to
    the parser's one-note-per-commit limit (its corrective footer rode a
    message stacking three run-together ``BREAKING CHANGE:`` lines, and
    only the last line survived).

    Once the manifest reaches the target release, the entry is verified
    against the shipped CHANGELOG's breaking bullets for that version
    instead. The census retires by construction at the cut (the
    carriers sit below the new tag and can never re-enter the range),
    and stays critical after it: a regeneration that drops a
    shipped entry fails here.
    """
    unmapped = [
        heading for heading in _breaking_flagged_sections() if _mapped_section_key(heading) is None
    ]
    assert not unmapped, (
        "docs/guides/upgrading.md flags these sections as breaking but "
        "tests/test_breaking_change_markers.py's policy map has no entry "
        "for them:\n  "
        + "\n  ".join(unmapped)
        + "\nA documented breaking change with no policy entry is a "
        "breaking change nobody has marked: add the mapping entry (with "
        "its target release) and the marker commit it points at, or "
        "soften the guide's flag."
    )
    released = _released_version()
    carriers: list[tuple[str, str]] | None = None
    for key, (target, needles) in _BREAKING_SECTION_ENTRIES.items():
        if _version_tuple(target) <= released:
            bullets = _shipped_breaking_bullets(target)
            assert bullets is not None, (
                f"the policy entry for {key!r} targets {target}, which the "
                f"manifest records as shipped, but CHANGELOG.md has no "
                f"[{target}] section: either the release cut without its "
                "generated changelog (fix the changelog) or the entry's "
                "target is wrong (prune or re-target the map entry)"
            )
            assert any(all(needle in bullet for needle in needles) for bullet in bullets), (
                f"the shipped [{target}] CHANGELOG section does not carry "
                f"{needles!r} for upgrading.md's {key!r}: a documented "
                "breaking change went missing from the shipped release "
                "notes. Re-mark it in a fresh commit on the pending "
                "release, or prune the stale map entry if the guide's "
                "promise was retracted."
            )
            continue
        if carriers is None:
            carriers = _carriers_in_range()
        assert any(all(needle in text for needle in needles) for _, text in carriers), (
            f"upgrading.md documents {key!r} as breaking for {target}, but "
            "no structurally unambiguous breaking marker in "
            f"{_release_range()} carries {needles!r}: release-please "
            "builds the notes purely from markers in the range it walks "
            "(at most one note per commit message), so this break would "
            "ship invisible. Mark it in a fresh commit: a `type!:` "
            "subject, a single-`BREAKING CHANGE:`-footer message, or one "
            "footer per BEGIN_NESTED_COMMIT block."
        )


def test_release_walk_depth_covers_the_release_range() -> None:
    """The walk's ``commit-search-depth`` must cover every commit since
    the last shipped release tag, or the notes are silently built from a
    window.

    release-please walks the newest ``commit-search-depth`` commits
    (default 500) looking for the last release SHA and stops there
    whether or not it found it (no error, just a shorter range). During
    the 0.3.0 cycle main grew past 500 commits since v0.2.2 and five
    real breaking markers (a3013fb, 07a6cfe, abc38d1, 12fd571, 801095c)
    fell out of the window without any failure. After each release the
    window resets to the new tag, so this only bites long cycles,
    which is exactly when it is silent.
    """
    config = json.loads(_RELEASE_CONFIG.read_text())
    depth = config.get("commit-search-depth", _DEFAULT_COMMIT_SEARCH_DEPTH)
    count = int(_git("rev-list", "--count", _release_range()).strip())
    assert depth >= count, (
        f"release-please walks at most {depth} commits "
        f"(commit-search-depth) but the release range "
        f"{_release_range()} is {count} commits long: the notes are "
        "silently built from a window that excludes the oldest "
        f"{count - depth} commits and every breaking marker they carry. "
        "Raise commit-search-depth in release-please-config.json (it "
        "only costs walk pages, and it resets to the new tag at each "
        "release)."
    )


def test_release_as_pin_is_a_pending_release_floor_not_a_stale_leftover() -> None:
    """The ``release-as`` pin floors the pending release at a
    minor-or-greater bump, and retires once that release ships.

    Why a floor while pending: the breaking changes reach ``main``
    through a merge whose strategy the release machinery does not
    control: a squash-merge collapses every ``fix!:`` marker on the
    stack into one hand-written message, and a marker-less history
    computes a PATCH bump, shipping user-visible breaks as a patch
    release. ``release-as`` fixes the floor regardless of how history
    lands.

    Why it must not linger: ``release-as`` forces the version on EVERY
    subsequent run until removed. Once the pinned version's tag exists,
    the pin is spent; leaving it in place re-pins the next release at
    an already-shipped version. The lifecycle this asserts: absent
    (normal marker-driven operation) or pending (above the manifest's
    last released minor) passes; equal to the manifest (the release PR
    is cutting the pinned release right now) passes; below or behind the
    manifest, or left in place after the pin's tag shipped, fails.
    """
    config = json.loads(_RELEASE_CONFIG.read_text())
    package = config["packages"]["."]
    manifest = json.loads(_RELEASE_MANIFEST.read_text())
    released = tuple(int(part) for part in str(manifest["."]).split("."))

    if "release-as" not in package:
        # Normal operation: the bump comes from the markers, whose
        # visibility the earlier guards in this file pin.
        return

    pinned_text = str(package["release-as"])
    pinned = tuple(int(part) for part in pinned_text.split("."))
    pinned_tag_resolved = (
        subprocess.run(  # noqa: S603  # Why: fixed literal argv shape: the tag name is this file's own config derivation, git from PATH, no shell.
            ["git", "rev-parse", "--verify", "--quiet", f"v{pinned_text}"],  # noqa: S607  # Why: git resolved from PATH, as elsewhere in this suite; fixed literal argv, no shell.
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
        ).returncode
        == 0
    )
    if pinned_tag_resolved:
        pytest.fail(
            f"release-please-config.json still pins release-as {pinned_text} "
            f"but v{pinned_text} has shipped (the manifest is at "
            f"{manifest['.']}); the pin is spent and must be removed: "
            "release-as forces the version on every subsequent run, so a "
            "lingering pin re-pins the next release at an already-shipped "
            "version."
        )
    assert pinned[:2] > released[:2] or pinned == released, (
        f"the hand-pinned release version {pinned_text} is neither a "
        f"minor-or-greater bump above the last released version "
        f"{manifest['.']} nor the release currently being cut: a "
        "patch-level floor ships the stack's breaking changes as a patch "
        "release, and a pin behind the manifest re-pins an old version."
    )
