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

The second half of the file guards a different hazard: ``taskq[oidc]``
installs two HTTP stacks that cannot see each other's mocks, so the OIDC
suite's respx interception rests on a bridge fixture that nothing else
would notice going missing. See the section comment there.
"""

import re
from pathlib import Path

import pytest

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


# ── Two-HTTP-stack hygiene ──────────────────────────────────────────────
# `taskq[oidc]` installs httpx AND httpx2, and they are invisible to each
# other's mocks. respx patches httpx's transport only, so anything reaching for
# httpx2 runs unmocked while the test still reads as mocked. The outbound
# guard in the root conftest.py stops such a call from reaching a real service;
# these tests stop the condition from arising silently in the first place.

_MOCK_HOST = "https://stack-probe.test.invalid"


async def test_respx_does_not_intercept_httpx2() -> None:
    """Pin the premise: respx cannot see httpx2, so the bridge is required.

    If this ever starts failing because respx grew httpx2 support, the
    ``_bridge_httpx2`` fixture in tests/test_sso_oidc.py becomes dead weight and
    should go. Until then it is the only reason the OIDC discovery and JWKS
    fetches are mocked at all.
    """
    httpx = pytest.importorskip("httpx")
    httpx2 = pytest.importorskip("httpx2")
    respx = pytest.importorskip("respx")

    router = respx.mock(assert_all_called=False)
    router.get(_MOCK_HOST).mock(return_value=httpx.Response(200, json={"ok": True}))
    router.start()
    try:
        async with httpx.AsyncClient() as patched:
            assert (await patched.get(_MOCK_HOST)).status_code == 200
        # Why raise-anything: the point is that the call escapes respx at all;
        # httpx2's transport error type is not part of the contract being pinned.
        with pytest.raises(Exception, match=r".") as excinfo:
            async with httpx2.AsyncClient() as unpatched:
                await unpatched.get(_MOCK_HOST)
        assert not isinstance(excinfo.value, AssertionError)
    finally:
        router.stop()


def test_authlib_oauth_client_is_on_the_stack_respx_patches() -> None:
    """Fail the moment an authlib upgrade moves its client onto httpx2.

    authlib 1.8 added ``integrations/httpx_client/_compat.py``, which does
    ``try: import httpx2 / except ImportError: import httpx as httpx2`` and so
    prefers httpx2 whenever it is importable — and httpx2 IS importable here.
    Its ``AsyncOAuth2Client`` then subclasses ``httpx2.AsyncClient``, respx
    stops applying to the token exchange with no error, and
    tests/test_sso_oidc.py starts calling the configured issuer for real. That
    is precisely how a sibling repo shipped a unit lane that talked to live
    Microsoft Entra while passing. A bump past 1.7.x must land the httpx2
    branch of tests/test_sso_oidc.py's bridge, not just re-pin the lock.
    """
    httpx = pytest.importorskip("httpx")
    pytest.importorskip("authlib")
    from authlib.integrations.httpx_client import AsyncOAuth2Client

    assert issubclass(AsyncOAuth2Client, httpx.AsyncClient), (
        "authlib's AsyncOAuth2Client is no longer an httpx.AsyncClient, so respx "
        "cannot intercept the OIDC token exchange. Extend the _bridge_httpx2 "
        "fixture in tests/test_sso_oidc.py to cover authlib's client too."
    )


def test_oidc_httpx2_bridge_is_still_present() -> None:
    """The OIDC suite's respx mocks are inert without this monkeypatch."""
    source = (_TESTS_DIR / "test_sso_oidc.py").read_text(encoding="utf-8")
    assert '"AsyncClient"' in source and "httpx2" in source, (
        "tests/test_sso_oidc.py no longer bridges httpx2 to httpx. Without it "
        "respx silently stops intercepting the discovery and JWKS fetches that "
        "src/taskq/web/admin/auth/oidc.py makes via `import httpx2 as httpx`."
    )
