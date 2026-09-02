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
form), and it also hosts the unit tests for the run-isolation naming seam
itself (``run_isolation_token`` / ``_module_db_name`` / the schema-name
helpers) — those tests necessarily set and assert ``PYTEST_XDIST_WORKER``,
which is the second reason the self-exemption exists. ``conftest.py``
files are excluded from the ``tests/`` scan: the conftest db-name helper
(``_module_db_name``) and the e2e schema helper use the worker id only as
ONE input to a per-module hash — the sanctioned worker-qualified-hash
pattern. ``src/taskq/testing/fixtures.py`` is allowlisted from the
worker-id scan for the same reason (``_schema_name_from_module`` /
``_schema_name_from_test`` hash the worker id together with the module
path / node id, so the name is never worker-only); every other file in
the published package is fully scanned.

The second half of the file guards a different hazard with the same shape:
``taskq[oidc]`` installs two HTTP client stacks, ``httpx`` and ``httpx2``,
which are invisible to each other's mocks. A mock that covers only some of
the stacks in use reads as a mock while part of the traffic leaves the
machine, or - worse - the suite compensates by substituting one stack for
the other and then tests a client production never constructs. See the
section comment there.
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
_TESTING_PKG_DIR = _TESTS_DIR.parent / "src" / "taskq" / "testing"
# Worker-qualified-hash exception: the worker id is one hash input among
# several (module path / test node id), never the whole identifier.
_TESTING_PKG_WORKER_ALLOWLIST = frozenset({"fixtures.py"})
# tests/http_mock.py documents the retired bridge verbatim and is the one
# module allowed to drive respx directly, so it is excluded alongside _SELF.
_HTTP_MOCK = _TESTS_DIR / "http_mock.py"

_PYTEST_XDIST_WORKER_RE = re.compile(r"PYTEST_XDIST_WORKER")
_MODULE_SCHEMA_CONST_RE = re.compile(r"^_?SCHEMA\s*=", re.MULTILINE)


def _test_files() -> list[Path]:
    return [p for p in _TESTS_DIR.rglob("*.py") if p != _SELF and p.name != "conftest.py"]


def _testing_pkg_files() -> list[Path]:
    return [
        p for p in _TESTING_PKG_DIR.rglob("*.py") if p.name not in _TESTING_PKG_WORKER_ALLOWLIST
    ]


def _stack_scanned_files() -> list[Path]:
    return [p for p in _test_files() if p != _HTTP_MOCK]


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


# ── Two-HTTP-stack hygiene ──────────────────────────────────────────────
# `taskq[oidc]` installs httpx AND httpx2, and one production code path uses
# both: src/taskq/web/admin/auth/oidc.py fetches discovery and JWKS over
# httpx2, while authlib's AsyncOAuth2Client performs the token exchange over
# whichever stack authlib binds to. Stock respx patches httpcore only, so it
# sees one half. tests/http_mock.py registers a respx mocker targeting every
# installed httpcore instead; these guards keep the suite pointed at it.
#
# Three earlier guards, added when the OIDC suite still bridged the stacks,
# are retired here. Each docstring below records which one it replaces and
# why, so the protection moves rather than disappearing.

_STACK_BRIDGE_RE = re.compile(r"setattr\(\s*httpx2?\s*,\s*[\"'](?:Async)?Client[\"']")
_BARE_RESPX_RE = re.compile(r"@respx\.mock|respx\.mock\(")


def test_no_test_file_bridges_one_http_stack_to_another() -> None:
    """No test may rebind one stack's client class to the other's.

    Retires ``test_oidc_httpx2_bridge_is_still_present``, which asserted the
    OPPOSITE: it pinned tests/test_sso_oidc.py's ``monkeypatch.setattr(httpx2,
    "AsyncClient", httpx.AsyncClient)`` in place, because without it respx
    silently stopped intercepting the discovery and JWKS fetches. That bridge
    is gone - respx is now aimed at httpcore2 as well - and keeping it would
    mean every OIDC test ran against a client class production never
    constructs, hiding any httpx2-only difference in timeouts, redirects, TLS
    verification, proxies or exception types.
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _stack_scanned_files()
        if _STACK_BRIDGE_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found an httpx/httpx2 client-class bridge in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nSubstituting one stack for the other makes mocks apply while the "
        "test exercises a client production never builds. Use "
        "tests.http_mock.mock_http, which mocks every installed stack in place."
    )


def test_http_mocking_is_routed_through_the_multi_stack_helper() -> None:
    """No test may call respx directly; stock respx covers httpcore only.

    Retires ``test_respx_does_not_intercept_httpx2``, which pinned respx's
    blindness to httpx2 as a premise so the bridge stayed justified. That
    premise no longer holds: tests/http_mock.py registers a mocker whose
    targets include httpcore2, so respx CAN see httpx2 - but only when aimed
    through that helper. Guarding the entry point is what keeps the coverage.
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _stack_scanned_files()
        if _BARE_RESPX_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found a direct respx.mock() call in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nrespx's default mocker patches httpcore only, so httpx2 traffic "
        "escapes it unmocked. Use tests.http_mock.mock_http instead."
    )


def test_mock_http_intercepts_every_installed_stack() -> None:
    """Every installed stack must actually be intercepted, not just declared.

    Retires ``test_authlib_oauth_client_is_on_the_stack_respx_patches``, which
    asserted that authlib's ``AsyncOAuth2Client`` subclasses ``httpx.AsyncClient``
    so that respx would apply to the token exchange. That pinned the suite to
    the deprecated stack: authlib 1.8 prefers httpx2 whenever it is importable,
    and the correct upgrade would have failed that assertion for the right
    reason. Which stack authlib picks no longer matters - what matters is that
    every stack present is covered, which is checked here by making a real
    request on each one.
    """
    pytest.importorskip("respx")
    import importlib

    from tests.http_mock import installed_stacks, mock_http, stacks_for

    stacks = installed_stacks()
    assert "httpx2" in stacks, "httpx2 is expected in every CI leg via the dev group"

    url = "https://stack-coverage.test.invalid/probe"
    with mock_http() as router:
        router.get(url).mock(return_value=importlib.import_module("httpx").Response(200))
        for name in sorted(stacks):
            with importlib.import_module(name).Client() as client:
                assert client.get(url).status_code == 200, f"{name} was not intercepted"
        assert stacks_for(url) == set(stacks), (
            f"expected every installed stack {sorted(stacks)} to reach the mock, "
            f"got {sorted(stacks_for(url))}"
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
