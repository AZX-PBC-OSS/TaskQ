"""Static hygiene guards for the test suite itself.

These are plain (non-PG, non-container) regression tests that grep the
``tests/`` tree for anti-patterns which have previously caused cross-test
and cross-worker schema collisions under ``pytest-xdist``:

- ``os.environ.get("PYTEST_XDIST_WORKER", ...)``-derived schema names give
  NO real isolation — every test file within one xdist worker resolves to
  the *same* string, so files sharing a worker mutually clobber each
  other's schema state.
- Module-level ``_SCHEMA = ...`` / ``SCHEMA = ...`` constants encode the
  same anti-pattern (or simply go stale) and should instead be sourced
  from the ``module_pg_schema`` / ``clean_pg_conn`` / ``clean_jobs_app``
  fixtures, or a per-test unique name (e.g. ``f"prefix_{new_base62()}"``).

New test files must not reintroduce either pattern. This file is excluded
from its own scan (it necessarily mentions the patterns in prose/regex
form), and it also hosts the unit tests for the run-isolation naming seam
itself (``run_isolation_token`` / ``_module_db_name`` / the schema-name
helpers) — those tests necessarily set and assert ``PYTEST_XDIST_WORKER``,
which is the second reason the self-exemption exists.
"""

import hashlib
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from taskq.testing.fixtures import (
    RUN_TOKEN_ENV_VAR,
    _schema_name_from_module,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra naming helper under test; private prefix scopes it to the testing package (same pattern as _create_worker).
    _schema_name_from_test,  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
    run_isolation_token,
)
from tests.conftest import (
    _module_db_name,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra naming helper under test; mirrors tests/e2e's imports of conftest helpers.
)

_TESTS_DIR = Path(__file__).parent
_SELF = Path(__file__)

_PYTEST_XDIST_WORKER_RE = re.compile(r"PYTEST_XDIST_WORKER")
_MODULE_SCHEMA_CONST_RE = re.compile(r"^_?SCHEMA\s*=", re.MULTILINE)


def _test_files() -> list[Path]:
    return [p for p in _TESTS_DIR.rglob("*.py") if p != _SELF and p.name != "conftest.py"]


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


# ── Run-isolation naming seam ──────────────────────────────────────
#
# Serial bare-``pytest`` runs share ``/tmp/pytest-of-<user>`` (the shared-pair
# state dir) across ALL invocations and checkouts, and the pre-token hash
# inputs were only ``(worker-or-"master", module)`` — invocation-invariant.
# Two overlapping serial runs of the same module therefore landed on the SAME
# database on the SAME shared pair, and each run's module teardown
# ``DROP DATABASE ... WITH (FORCE)`` killed the other run's live pools
# mid-test (redteam-reproduced: both runs rc=1 with pool-init failures on
# tq_db_5a19dc6e3f4c). The run token mixes the invocation-unique basetemp
# dir name into every hash input; under xdist the worker id already is
# invocation-unique, so behavior there is unchanged.


class _StubModuleRequest:
    """The ``request.module.__name__`` / ``request.node.nodeid`` surfaces the
    naming helpers read — lets the tests vary hash inputs without building a
    real ``FixtureRequest``."""

    def __init__(self) -> None:
        self.module = SimpleNamespace(__name__="tests.test_suite_hygiene")
        self.node = SimpleNamespace(nodeid="tests/test_suite_hygiene.py::test_stub")


def _legacy_module_db_hash(worker: str) -> str:
    """The pre-token hash of this module for *worker* — pins xdist inputs."""
    full = "tests_test_suite_hygiene"
    return "tq_db_" + hashlib.md5(f"{worker}_{full}".encode()).hexdigest()[:12]  # noqa: S324  # Why: mirrors the non-cryptographic naming hash under test.


def test_module_db_names_diverge_across_serial_run_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent serial runs of this module hash to DIFFERENT databases
    and schemas — the per-run token is part of every hash input."""
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    stub = cast(pytest.FixtureRequest, _StubModuleRequest())
    db_names: set[str] = set()
    module_schemas: set[str] = set()
    test_schemas: set[str] = set()
    for token in ("pytest-41", "pytest-42"):
        monkeypatch.setenv(RUN_TOKEN_ENV_VAR, token)
        db_names.add(_module_db_name(stub))
        module_schemas.add(_schema_name_from_module(stub))
        test_schemas.add(_schema_name_from_test(stub))
    assert len(db_names) == 2, f"module database names collide: {db_names}"
    assert len(module_schemas) == 2, f"module schema names collide: {module_schemas}"
    assert len(test_schemas) == 2, f"test schema names collide: {test_schemas}"


def test_run_isolation_token_prefers_the_published_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The published run token wins over the xdist worker id — the conftest
    derives the token once at session start and the helpers read that seam."""
    monkeypatch.setenv(RUN_TOKEN_ENV_VAR, "pytest-41")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    assert run_isolation_token() == "pytest-41"


def test_xdist_hash_inputs_are_unchanged_by_the_token_seam(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under xdist the hash inputs stay exactly the worker id — the token
    seam must not perturb per-worker names (their state dir is already
    per-invocation, so there is nothing to fix there). Covers both the bare
    worker env (direct library use) and the conftest-published token=gwK
    shape an actual xdist worker sees."""
    stub = cast(pytest.FixtureRequest, _StubModuleRequest())
    for with_published_token in (False, True):
        monkeypatch.delenv(RUN_TOKEN_ENV_VAR, raising=False)
        monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
        if with_published_token:
            monkeypatch.setenv(RUN_TOKEN_ENV_VAR, "gw0")
        assert _module_db_name(stub) == _legacy_module_db_hash("gw0")


def test_session_publishes_run_isolation_token(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """The session conftest publishes the token before any naming helper
    runs: the xdist worker id under xdist, else the invocation-unique
    basetemp dir name (e.g. ``pytest-41``) — the value two overlapping
    serial runs can never share."""
    token = os.environ.get(RUN_TOKEN_ENV_VAR)
    assert token is not None, "session fixture did not publish the run token"
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    expected = worker if worker is not None else tmp_path_factory.getbasetemp().name
    assert token == expected
