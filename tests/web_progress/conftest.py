"""Pytest fixtures for the taskq.web.progress test suite.

Pytest discovers conftest.py fixtures in the test file's directory and all
parent directories.  Test modules inside ``tests/web_progress/`` automatically
see every fixture defined here.
"""

import pytest

# ── Autouse fixtures ──────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Set TASKQ_ENVIRONMENT=dev for all web_progress tests so
    create_router's fail-closed auth check does not raise.

    These tests exercise SSE mechanics (subscribe-before-query, reconnect,
    keepalives, teardown, connection caps) against stub pools and pubsubs —
    none of them assert on authentication, and mounting the router is a
    prerequisite for all of them. The gate itself has its own test file
    (test_auth_gate.py), whose non-dev cases override this with their own
    monkeypatch.setenv, exactly as the web_admin suite's equivalent fixture
    allows.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
