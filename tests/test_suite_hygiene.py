"""Static hygiene guards for the test suite itself.

These are plain (non-PG, non-container) regression tests that grep the
``tests/`` tree AND the published ``src/taskq/testing/`` package for
anti-patterns which have previously caused cross-test and cross-worker
schema collisions under ``pytest-xdist``:

- ``os.environ.get("PYTEST_XDIST_WORKER", ...)``-derived schema names give
  NO real isolation — every test file within one xdist worker resolves to
  the *same* string, so files sharing a worker mutually clobber each
  other's schema state. For the PUBLISHED testing package this is worse:
  a fixed per-worker default reaches consumer suites on shared-database
  models, where it clobbers across modules.
- Module-level ``_SCHEMA = ...`` / ``SCHEMA = ...`` constants encode the
  same anti-pattern (or simply go stale) and should instead be sourced
  from the ``module_pg_schema`` / ``clean_pg_conn`` / ``clean_jobs_app``
  fixtures, or a per-test unique name (e.g. ``f"prefix_{new_base62()}"``).

New test files must not reintroduce either pattern. This file is excluded
from its own scan (it necessarily mentions the patterns in prose/regex
form). ``conftest.py`` files are excluded from the ``tests/`` scan: the
conftest db-name helper (``_module_db_name``) and the e2e schema helper use
the worker id only as ONE input to a per-module hash — the sanctioned
worker-qualified-hash pattern. ``src/taskq/testing/fixtures.py`` is
allowlisted from the worker-id scan for the same reason
(``_schema_name_from_module`` / ``_schema_name_from_test`` hash the worker
id together with the module path / node id, so the name is never
worker-only); every other file in the published package is fully scanned.
"""

import re
from pathlib import Path

_TESTS_DIR = Path(__file__).parent
_SELF = Path(__file__)
_TESTING_PKG_DIR = _TESTS_DIR.parent / "src" / "taskq" / "testing"
# Worker-qualified-hash exception: the worker id is one hash input among
# several (module path / test node id), never the whole identifier.
_TESTING_PKG_WORKER_ALLOWLIST = frozenset({"fixtures.py"})

_PYTEST_XDIST_WORKER_RE = re.compile(r"PYTEST_XDIST_WORKER")
_MODULE_SCHEMA_CONST_RE = re.compile(r"^_?SCHEMA\s*=", re.MULTILINE)


def _test_files() -> list[Path]:
    return [p for p in _TESTS_DIR.rglob("*.py") if p != _SELF and p.name != "conftest.py"]


def _testing_pkg_files() -> list[Path]:
    return [
        p for p in _TESTING_PKG_DIR.rglob("*.py") if p.name not in _TESTING_PKG_WORKER_ALLOWLIST
    ]


def test_no_pytest_xdist_worker_derived_schema_names() -> None:
    """No test file may derive a schema/identifier name from
    ``PYTEST_XDIST_WORKER`` — it does not provide cross-file isolation
    within a worker (see module docstring). Use ``module_pg_schema`` /
    ``clean_pg_conn`` / ``clean_jobs_app`` or a unique per-test name
    instead.
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _test_files()
        if _PYTEST_XDIST_WORKER_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found PYTEST_XDIST_WORKER-derived schema/name patterns in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse the module_pg_schema / clean_pg_conn / clean_jobs_app fixtures, "
        "or a unique per-test name (e.g. f'prefix_{new_base62()}'), instead."
    )


def test_testing_pkg_no_pytest_xdist_worker_derived_schema_names() -> None:
    """The PUBLISHED ``taskq.testing`` package must not derive names from
    ``PYTEST_XDIST_WORKER`` either — a fixed per-worker default there leaks
    into consumer suites on shared-database models. ``fixtures.py`` is
    allowlisted (worker-qualified hashes only — see module docstring).
    """
    offenders = [
        str(p.relative_to(_TESTING_PKG_DIR))
        for p in _testing_pkg_files()
        if _PYTEST_XDIST_WORKER_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found PYTEST_XDIST_WORKER-derived schema/name patterns in published "
        "taskq.testing package:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse a per-call unique name (e.g. f'tq_{new_base62()}'.lower()), or a "
        "worker-qualified hash like fixtures.py's _schema_name_from_module, instead. "
        "A fixed per-worker name is shared by every caller in the process."
    )


def test_no_module_level_schema_constant() -> None:
    """No test file may define a module-level ``_SCHEMA`` / ``SCHEMA``
    constant. These tend to be shared (and stale) across many tests in
    a file; prefer fixture-derived or per-test-local schema names.
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _test_files()
        if _MODULE_SCHEMA_CONST_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found module-level _SCHEMA/SCHEMA constant(s) in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse module_pg_schema.schema_name (or a local per-test/per-call "
        "variable) instead of a module-level constant."
    )


def test_testing_pkg_no_module_level_schema_constant() -> None:
    """Same module-level ``_SCHEMA`` / ``SCHEMA`` ban for the published
    ``taskq.testing`` package (no allowlist — the pattern is never valid).
    """
    offenders = [
        str(p.relative_to(_TESTING_PKG_DIR))
        for p in _TESTING_PKG_DIR.rglob("*.py")  # no allowlist — never valid
        if _MODULE_SCHEMA_CONST_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found module-level _SCHEMA/SCHEMA constant(s) in published "
        "taskq.testing package:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse a per-call unique name instead of a module-level constant."
    )
