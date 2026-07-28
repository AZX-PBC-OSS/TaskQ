"""Structural gate for the containerized e2e tier (tests/e2e/).

Regression: a command-line ``-m`` REPLACES the addopts marker expression
rather than combining with it, so a marker-only ``-m "not e2e"`` gate let
``-m "not integration"`` (make test-fast, README fast loop) and the CI legs
passing ``-m "not redis"`` collect the whole e2e tier — docker image and
wheel builds on every run. The gate must be structural: tests/e2e is never
collected unless ``--e2e`` is passed, regardless of any ``-m`` expression.

The positive case (opt-in collects the tier) requires the ``e2e``
dependency group (containerspec); it skips cleanly elsewhere.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _collect(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 # Why: argv is fully static (current interpreter + fixed pytest flags) plus this test's own literal marker expressions; no untrusted input
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *args],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


@pytest.mark.parametrize("marker_expr", ["not integration", "not redis"])
def test_cli_marker_expression_never_collects_e2e(marker_expr: str) -> None:
    """Fast-loop / CI-leg ``-m`` expressions select zero e2e tests."""
    proc = _collect("-m", marker_expr)
    assert proc.returncode in (0, 5), proc.stderr[-2000:]
    assert "tests/e2e/" not in proc.stdout


def test_e2e_dir_not_collected_without_opt_in() -> None:
    """Pointing pytest directly at tests/e2e without ``--e2e`` collects nothing."""
    proc = _collect("tests/e2e")
    assert proc.returncode == 5, proc.stdout[-2000:]
    assert "tests/e2e/" not in proc.stdout


def test_e2e_marker_alone_does_not_open_tier() -> None:
    """``-m e2e`` without ``--e2e`` still collects nothing — the gate is structural."""
    proc = _collect("-m", "e2e", "tests/e2e")
    assert proc.returncode == 5, proc.stdout[-2000:]
    assert "tests/e2e/" not in proc.stdout


@pytest.mark.skipif(
    importlib.util.find_spec("containerspec") is None,
    reason="e2e dependency group not installed (uv sync --group e2e)",
)
def test_e2e_opt_in_collects_tier() -> None:
    """``--e2e -m e2e`` collects the tier (the make test-e2e path)."""
    proc = _collect("--e2e", "-m", "e2e", "tests/e2e")
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "tests/e2e/" in proc.stdout
