"""Released migration files are frozen byte-for-byte.

The ledger records a SHA-256 of each migration's rendered SQL when it is
applied, and the runner logs ``migration-checksum-drift`` on every later
apply whose recomputed checksum differs. Any edit to a shipped file — a
comment, whitespace, a reworded header — makes every existing deployment
log that warning for the file forever, and a tamper warning that always
fires is one operators learn to ignore. ``tests/data/released_migrations.sha256``
pins the bytes of every file a release has shipped; this test fails the
moment one of them changes. Files not listed are new to the unreleased
tree and are added to the manifest when they ship.
"""

from __future__ import annotations

import hashlib
from importlib import resources
from pathlib import Path

import pytest

_MANIFEST = Path(__file__).parent / "data" / "released_migrations.sha256"


def _manifest() -> dict[str, str]:
    pinned: dict[str, str] = {}
    for raw in _MANIFEST.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        digest, _, filename = line.partition("  ")
        assert len(digest) == 64 and filename, f"malformed manifest line: {raw!r}"
        pinned[filename] = digest
    assert pinned, "the manifest lists no files"
    return pinned


def _shipped_bytes(filename: str) -> bytes:
    return resources.files("taskq.migrations").joinpath(filename).read_bytes()


@pytest.mark.parametrize("filename", sorted(_manifest()))
def test_released_migration_file_bytes_are_unchanged(filename: str) -> None:
    pinned = _manifest()[filename]
    actual = hashlib.sha256(_shipped_bytes(filename)).hexdigest()
    assert actual == pinned, (
        f"{filename} shipped in a release and its bytes have changed "
        f"(sha256 {actual}, pinned {pinned}); every deployment that applied it "
        "would log migration-checksum-drift on its next migrate. Restore the file "
        "and fix forward with a new migration."
    )


def test_manifest_lists_only_files_the_package_ships() -> None:
    shipped = {
        entry.name
        for entry in resources.files("taskq.migrations").iterdir()
        if entry.is_file() and entry.name.endswith(".sql")
    }
    missing = sorted(set(_manifest()) - shipped)
    assert not missing, f"manifest names migration files the package no longer ships: {missing}"
