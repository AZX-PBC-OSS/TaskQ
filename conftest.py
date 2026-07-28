"""Repository-root pytest configuration.

Holds the e2e-tier collection gate. It MUST live at the root: conftest
files are registered as their directories are visited, so a gate in
tests/conftest.py registers too late to reliably stop the tier from being
collected on every invocation shape.
"""

from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help=(
            "Collect the containerized e2e tier (tests/e2e). Off by default; "
            "requires the e2e dependency group (uv sync --group e2e) and Docker."
        ),
    )


def _is_e2e_path(path: Path) -> bool:
    return path.name == "e2e" and path.parent.name == "tests"


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Keep the e2e tier out of directory recursion unless ``--e2e`` is passed.

    A command-line ``-m`` REPLACES the addopts marker expression instead of
    combining with it, so a marker-only gate (``-m "not e2e"`` in addopts)
    silently opened the tier to every ``-m "not integration"`` /
    ``-m "not redis"`` run. Ignoring the directory at collection is
    independent of ``-m``.
    """
    if config.getoption("--e2e"):
        return None
    if _is_e2e_path(collection_path):
        return True
    return None


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Explicit-arg backstop for the e2e gate.

    ``pytest_ignore_collect`` is not consulted for paths passed explicitly
    on the command line (``pytest tests/e2e``), so an explicit-arg run
    without ``--e2e`` still collects the tier. Drop those items here.
    """
    if config.getoption("--e2e"):
        return
    items[:] = [
        item
        for item in items
        if not (item.path.parent.name == "e2e" and item.path.parent.parent.name == "tests")
    ]
