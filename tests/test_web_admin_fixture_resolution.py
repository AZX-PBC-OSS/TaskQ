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
guards here pin BOTH orderings, plus a probe of pytest's own fixture
resolution under interleaving that avoids the upstream bug's trigger (a
nested conftest) and therefore runs unconditionally.
"""

import os
import subprocess
import sys
from pathlib import Path

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


def _run_pytest(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run this interpreter's pytest on *args* from *cwd*."""
    return subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the subprocess IS the system under test.
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *args],
        cwd=cwd,
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
    _assert_no_fixture_errors(_run_pytest(_REPO_ROOT, *_INTERLEAVED_ARGS))


def test_web_admin_fixtures_root_file_first_control() -> None:
    """The control ordering from the issue: root file first, web_admin adjacent.

    Passed before the fix too; pinned so the guard covers both shapes and an
    over-eager "fix" cannot trade one ordering for the other.
    """
    _assert_no_fixture_errors(_run_pytest(_REPO_ROOT, *_CONTROL_ARGS))


# The upstream bug is specific to fixtures defined in a NESTED conftest.py:
# re-collection of the revisited directory builds a second ``Directory`` node
# and fixture registration matches by node identity, so the nested conftest's
# registrations vanish. Fixtures that do not ride a re-collected Directory
# node - the root conftest.py and each test module's own fixtures - resolve
# under the same interleaving on every pytest this suite pins (verified
# against 9.1.1 and 8.4.2). The probe below pins that scenario; it cannot
# skip. Once the pin moves past the release that ships the 9.1.x backport
# (pytest-dev/pytest#14968, merged 2026-09-06 but unreleased as of 9.1.1), a
# nested-conftest variant of this scenario can be added to probe the upstream
# fix itself.
def test_pytest_interleaved_arguments_resolve_fixtures(tmp_path: Path) -> None:
    """Pin order-independent fixture resolution WITHOUT a nested conftest.

    Mirrors the upstream issue's layout minus the nested conftest: the root
    ``conftest.py`` defines a fixture and each nested test file defines its
    own module-level fixture. A file REVISITED non-adjacently in the argument
    list must resolve both, in both orderings.
    """
    (tmp_path / "tests" / "services").mkdir(parents=True)
    (tmp_path / "tests" / "conftest.py").write_text(
        'import pytest\n\n\n@pytest.fixture\ndef root_fixture():\n    return "root"\n'
    )
    (tmp_path / "tests" / "services" / "test_a.py").write_text(
        "import pytest\n\n\n"
        '@pytest.fixture\ndef module_fixture_a():\n    return "mod-a"\n\n\n'
        "def test_a(module_fixture_a, root_fixture):\n"
        "    assert module_fixture_a == 'mod-a'\n"
        "    assert root_fixture == 'root'\n"
    )
    (tmp_path / "tests" / "services" / "test_b.py").write_text(
        "import pytest\n\n\n"
        '@pytest.fixture\ndef module_fixture_b():\n    return "mod-b"\n\n\n'
        "def test_b(module_fixture_b, root_fixture):\n"
        "    assert module_fixture_b == 'mod-b'\n"
        "    assert root_fixture == 'root'\n"
    )
    (tmp_path / "tests" / "test_top.py").write_text(
        "def test_top(root_fixture):\n    assert root_fixture == 'root'\n"
    )

    _assert_no_fixture_errors(
        _run_pytest(
            tmp_path,
            "tests/services/test_a.py",
            "tests/test_top.py",
            "tests/services/test_b.py",
        )
    )
    _assert_no_fixture_errors(
        _run_pytest(
            tmp_path,
            "tests/test_top.py",
            "tests/services/test_a.py",
            "tests/services/test_b.py",
        )
    )
