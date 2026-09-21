"""Upgrade-path attack: the dependency-floor claims stay installable truths.

The floors (pyproject's ``>=`` bounds) are what a clean install resolves:
a floor that stopped working is a red, and a floor that moved in
pyproject without the docs moving (or vice versa) is a silent drift
between the two places the floor is claimed. This pin freezes the five
tested floors — the exact versions a clean venv install was executed
against (import battery + ``taskq migrate up`` + the worker CLI + the
README quickstart round-trip, Python 3.12) — against both surfaces.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

#: The floors exactly as executed (atk/upgrade-paths, clean venv, 3.12):
#: asyncpg==0.31.0, redis==8.0.1, pydantic==2.13.4, uuid-utils==1.0.0,
#: fastapi==0.140.0 — full import battery, migrate up, worker run, and the
#: README quickstart all pass at these versions.
TESTED_FLOORS: dict[str, str] = {
    "asyncpg": "0.31.0",
    "redis": "8.0.1",
    "pydantic": "2.13.4",
    "uuid-utils": "1.0.0",
    "fastapi": "0.140.0",
}

_PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _floor_for(package: str, requires: list[str]) -> str:
    matches = [
        r for r in requires if r.split("[")[0].split(">")[0].split("=")[0].strip() == package
    ]
    assert matches, f"pyproject declares no dependency on {package!r}"
    match = matches[0]
    # The floor is the >= bound; an upper bound may ride along.
    for part in match.split(","):
        part = part.strip()
        if ">=" in part:
            return part.split(">=", 1)[1]
    raise AssertionError(f"dependency {match!r} carries no >= floor")


def test_pyproject_floors_are_the_executed_ones() -> None:
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    requires = data["project"]["dependencies"]
    extras: dict[str, list[str]] = data["project"]["optional-dependencies"]
    redis_extra = extras["redis"]
    fastapi_extra = extras["fastapi"]

    for package, floor in TESTED_FLOORS.items():
        if package == "redis":
            assert _floor_for(package, redis_extra) == floor, (
                f"the redis extra's floor moved off the executed floor {floor}"
            )
            continue
        if package == "fastapi":
            assert _floor_for(package, fastapi_extra) == floor, (
                f"the fastapi extra's floor moved off the executed floor {floor}"
            )
            continue
        assert _floor_for(package, requires) == floor, (
            f"{package}'s floor moved off the executed floor {floor}; "
            "re-run the floor battery before moving it"
        )


def test_documented_redis_floor_matches_pyproject() -> None:
    """docs/index.md's extras table is the floor's other claim surface."""
    index = (Path(__file__).resolve().parents[1] / "docs" / "index.md").read_text(encoding="utf-8")
    assert f"`redis>={TESTED_FLOORS['redis']}`" in index, (
        "docs/index.md's redis floor no longer matches the tested floor"
    )


@pytest.mark.parametrize(("package", "floor"), sorted(TESTED_FLOORS.items()))
def test_floor_version_is_a_valid_pep440_bound(package: str, floor: str) -> None:
    from packaging.version import Version

    Version(floor)  # raises on an invalid version literal
