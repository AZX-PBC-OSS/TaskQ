"""Every pyproject extra must appear in the docs' extras tables — and vice versa.

The extras surface is defined once, in pyproject's
``[project.optional-dependencies]``, and described in three places (README's
Installation section, docs/getting-started/installation.md,
docs/getting-started/quick-start.md). The descriptions are hand-maintained, so
they drift: an extra added to pyproject silently has no docs (an operator
cannot discover it), and a doc row naming an extra pyproject does not have is
an install command that resolves to nothing.

This pins BOTH directions: every pyproject extra is named in each table, and
every extra the tables name exists in pyproject.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_PYPROJECT = _REPO_ROOT / "pyproject.toml"

# README's table uses the bare-extra spelling (``[redis]``); the mkdocs pages
# use ``taskq-py[redis]``. Accept either so the pin constrains PRESENCE, not
# house spelling.
_BARE_EXTRA = re.compile(r"\[(?P<name>[a-z]+)\]")
_QUALIFIED_EXTRA = re.compile(r"taskq-py\[(?P<name>[a-z]+)(?:,[a-z]+)*\]")


def _pyproject_extras() -> dict[str, list[str]]:
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    return data["project"]["optional-dependencies"]


def _documented_extras(page: Path) -> set[str]:
    text = page.read_text(encoding="utf-8")
    names = {m.group("name") for m in _QUALIFIED_EXTRA.finditer(text)}
    # Bare ``[name]`` spellings only count inside table rows (README's extras
    # table); outside tables the bare brackets are markdown link text
    # (``[uv](https://...)``), not extras.
    table_lines = (line for line in text.splitlines() if line.lstrip().startswith("|"))
    for line in table_lines:
        names |= {m.group("name") for m in _BARE_EXTRA.finditer(line)}
    return names


@pytest.mark.parametrize(
    "page",
    [
        _REPO_ROOT / "README.md",
        _REPO_ROOT / "docs" / "getting-started" / "installation.md",
        _REPO_ROOT / "docs" / "getting-started" / "quick-start.md",
    ],
    ids=lambda p: p.name,
)
def test_every_pyproject_extra_is_documented(page: Path) -> None:
    documented = _documented_extras(page)
    missing = sorted(set(_pyproject_extras()) - documented)
    assert not missing, f"{page.name} never names extra(s) {missing}"


def test_every_documented_extra_exists_in_pyproject() -> None:
    real = set(_pyproject_extras())
    for page in [
        _REPO_ROOT / "README.md",
        _REPO_ROOT / "docs" / "getting-started" / "installation.md",
        _REPO_ROOT / "docs" / "getting-started" / "quick-start.md",
    ]:
        phantom = _documented_extras(page) - real
        assert not phantom, (
            f"{page.name} names extra(s) {sorted(phantom)} that pyproject does not have"
        )
