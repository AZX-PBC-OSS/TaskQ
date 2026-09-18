"""Regression guards for web_admin fixture resolution under interleaved args.

pytest 9.1.1 loses a nested conftest's fixtures for a file REVISITED
non-adjacently in the command-line argument list: with
``pytest tests/web_admin/a.py tests/test_root.py tests/web_admin/b.py``
every test in ``b.py`` errors with "fixture 'stub_pool' not found" while the
adjacent ordering (root file first, web_admin files together) passes. The
issue's verified repro on this suite: mixed order 62 passed / 28 errors,
root-file-first control 63/63. Upstream: pytest-dev/pytest#14971 (collection
creates a second ``Directory`` node for the revisited directory and fixture
registration is matched by node identity, so fixtures registered under the
first instance are invisible to items collected under the second).

Fix: the web_admin fixtures live in :mod:`tests.web_admin._fixtures` and are
registered from ``tests/conftest.py`` (loaded for every test regardless of
argument order), so resolution no longer depends on conftest adjacency. The
guards here pin BOTH orderings, plus a probe of the upstream pytest behavior
that activates once the installed pytest carries the upstream fix.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]

#: The exact mixed argument list from the issue's verified repro: a web_admin
#: file, a root-level file, then a SECOND web_admin file. The revisited
#: directory (``tests/web_admin``) is what triggers the upstream node-identity
#: mismatch; every web_admin test in the last file errored before the fix.
_INTERLEAVED_ARGS = (
    "tests/web_admin/test_factory.py",
    "tests/test_zero_value_convention.py",
    "tests/web_admin/test_queues.py",
)

#: Root-file-first control: same files, web_admin ones adjacent.
_CONTROL_ARGS = (
    "tests/test_zero_value_convention.py",
    "tests/web_admin/test_factory.py",
    "tests/web_admin/test_queues.py",
)


def _run_pytest(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the repo's own pytest on *args* from the repo root."""
    return subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the subprocess IS the system under test.
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *args],
        cwd=_REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=600,
    )


def _assert_no_fixture_errors(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, (
        f"pytest exited {result.returncode}\n--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}"
    )
    summary = result.stdout.strip().splitlines()[-1]
    assert " error" not in summary, f"setup errors in summary {summary!r}:\n{result.stdout}"


def test_web_admin_fixtures_survive_interleaved_arguments() -> None:
    """The issue's repro: web_admin files revisited non-adjacently.

    Before the fix (pytest 9.1.1, fixtures in tests/web_admin/conftest.py):
    every test in the revisited file errored with "fixture 'stub_pool' not
    found". Registration from tests/conftest.py makes resolution independent
    of argument order.
    """
    _assert_no_fixture_errors(_run_pytest(*_INTERLEAVED_ARGS))


def test_web_admin_fixtures_root_file_first_control() -> None:
    """The control ordering from the issue: root file first, web_admin adjacent.

    Passed before the fix too; pinned so the guard covers both shapes and an
    over-eager "fix" cannot trade one ordering for the other.
    """
    _assert_no_fixture_errors(_run_pytest(*_CONTROL_ARGS))


# Upstream behavior (pytest-dev/pytest#14971): the fix landed on pytest main
# after 9.1.1 and was backported to the 9.1.x branch (pytest-dev/pytest#14968,
# merged 2026-09-06), but no released pytest carried it when this guard was
# written. Bump this once the repo's pin moves to a release that ships the
# backport (the next 9.1.x patch or 9.2.0), and the probe below starts
# asserting instead of skipping.
_UPSTREAM_FIXED = (9, 2, 0)


def _pytest_version_tuple() -> tuple[int, ...]:
    return tuple(int(part) for part in pytest.__version__.split(".")[:2])


def test_upstream_pytest_interleaved_conftest_behavior(tmp_path: Path) -> None:
    """Probe the minimal upstream #14971 scenario against the installed pytest.

    Once pytest carries the upstream fix the probe asserts collection is
    order-independent WITHOUT our restructuring; while the installed pytest is
    known-affected it skips (the repo-level guards above prove our suite is
    order-independent anyway).
    """
    if _pytest_version_tuple() < _UPSTREAM_FIXED:
        pytest.skip(
            f"pytest {pytest.__version__} predates the pytest-dev/pytest#14971 fix "
            "(backport pytest-dev/pytest#14968); nested conftest fixtures are "
            "expected to drop under interleaved file arguments. _UPSTREAM_FIXED "
            "must be bumped when the pin moves past it."
        )

    (tmp_path / "tests" / "services").mkdir(parents=True)
    (tmp_path / "tests" / "conftest.py").write_text(
        "import pytest\n"
        "\n"
        "\n"
        "@pytest.fixture\n"
        "def root_fixture():\n"
        '    return "root"\n'
    )
    (tmp_path / "tests" / "services" / "conftest.py").write_text(
        "import pytest\n"
        "\n"
        "\n"
        "@pytest.fixture\n"
        "def nested_fixture():\n"
        '    return "nested"\n'
    )
    (tmp_path / "tests" / "services" / "test_a.py").write_text(
        "def test_a(nested_fixture):\n    assert nested_fixture == 'nested'\n"
    )
    (tmp_path / "tests" / "services" / "test_b.py").write_text(
        "def test_b(nested_fixture):\n    assert nested_fixture == 'nested'\n"
    )
    (tmp_path / "tests" / "test_top.py").write_text(
        "def test_top(root_fixture):\n    assert root_fixture == 'root'\n"
    )

    result = subprocess.run(  # Why: fixed argv, no shell; the probe IS the system under test.
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "tests/services/test_a.py",
            "tests/test_top.py",
            "tests/services/test_b.py",
        ],
        cwd=tmp_path,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=600,
    )
    _assert_no_fixture_errors(result)
