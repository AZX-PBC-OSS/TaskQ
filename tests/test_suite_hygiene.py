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
form).

The second half of the file guards a different hazard with the same shape:
``taskq[oidc]`` installs two HTTP client stacks, ``httpx`` and ``httpx2``,
which are invisible to each other's mocks. A mock that covers only some of
the stacks in use reads as a mock while part of the traffic leaves the
machine, or - worse - the suite compensates by substituting one stack for
the other and then tests a client production never constructs. See the
section comment there.
"""

import re
from pathlib import Path

import pytest

_TESTS_DIR = Path(__file__).parent
_SELF = Path(__file__)
# tests/http_mock.py documents the retired bridge verbatim and is the one
# module allowed to drive respx directly, so it is excluded alongside _SELF.
_HTTP_MOCK = _TESTS_DIR / "http_mock.py"

_PYTEST_XDIST_WORKER_RE = re.compile(r"PYTEST_XDIST_WORKER")
_MODULE_SCHEMA_CONST_RE = re.compile(r"^_?SCHEMA\s*=", re.MULTILINE)


def _test_files() -> list[Path]:
    return [p for p in _TESTS_DIR.rglob("*.py") if p != _SELF and p.name != "conftest.py"]


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
