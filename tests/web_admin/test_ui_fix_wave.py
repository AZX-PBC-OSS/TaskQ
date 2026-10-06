"""The consolidated admin-UI fix wave's pins (B1-B5, U1-U7, D1/D2/D4).

Every pin is the mutation-checked teeth of one finding from the admin-UI
audit: revert the fix and the pin reds. The findings live in the review
ledger; this file holds the UI-side half, ``tests/test_api_fix_wave.py``
the API-side half.

B1 - the shipped deployments never passed a backend to ``create_router``,
     so every backend-mediated mutation button (cancel / retry / run-now)
     posted to a route that answered 503. The server-side half: a
     deployment with no backend must not RENDER the dead buttons at all
     (the templates read the bundle off the render context).
B2 - ``TASKQ_ADMIN_ACTIONS_ENABLED=false`` (the default) still rendered
     every action form, and the POST refusal came out as a CSRF error
     (the CSRF dependency ran before the enabled-check) - a config state
     reported as a token problem. The enabled-check now runs first with
     an actionable message, and disabled deployments render a banner and
     no forms.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from taskq._ids import new_uuid

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state

from . import StubPool

pytestmark = [pytest.mark.fastapi]

_ACTIONS_BANNER = "Actions are disabled on this deployment (TASKQ_ADMIN_ACTIONS_ENABLED=false)"

_JOB_ID = new_uuid()


class _JobDetailPool(StubPool):
    """A stub pool whose connection answers the job-detail page's reads.

    The job fetch returns a pending live row (so the cancel form's
    condition is met); every other read is empty, like :class:`StubPool`.
    """

    def __init__(self) -> None:
        self.job: dict[str, Any] | None = {
            "id": _JOB_ID,
            "actor": "test_actor",
            "queue": "default",
            "status": "pending",
            "created_at": "2026-01-01T00:00:00+00:00",
            "scheduled_at": "2026-01-01T00:00:00+00:00",
            "started_at": None,
            "finished_at": None,
            "attempt": 0,
            "max_attempts": 3,
            "retry_kind": "transient",
            "priority": 0,
            "identity_key": None,
            "fairness_key": None,
            "locked_by_worker": None,
            "lock_expires_at": None,
            "cancel_requested_at": None,
            "progress_state": None,
            "progress_seq": 0,
            "payload": None,
            "metadata": None,
            "result": None,
            "error_class": None,
            "error_message": None,
            "error_traceback": None,
            "trace_id": None,
            "span_id": None,
            "tags": [],
            "schedule_to_close": None,
            "start_to_close": None,
            "heartbeat_timeout": None,
            "cancel_phase": 0,
            "result_size_bytes": None,
        }

    async def _maybe_job(self, query: str, *args: object) -> dict[str, Any] | None:
        if 'FROM "taskq".jobs WHERE id = $1' in query:
            return self.job
        return None


class _JobDetailConnection:
    """Connection stub routing each admin read to the right canned answer."""

    def __init__(self, pool: _JobDetailPool) -> None:
        self._pool = pool

    def transaction(self) -> Any:
        from . import StubTransaction

        return StubTransaction()

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        return []

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        return await self._pool._maybe_job(query, *args)

    async def fetchval(self, query: str, *args: object) -> Any:
        from datetime import UTC, datetime

        if "clock_timestamp()" in query:
            # The clock-offset probe must see a datetime, the shape the
            # real database answers with (see StubConnection.fetchval).
            return datetime.now(UTC)
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _JobDetailAcquire:
    def __init__(self, pool: _JobDetailPool) -> None:
        self._pool = pool

    async def __aenter__(self) -> _JobDetailConnection:
        return _JobDetailConnection(self._pool)

    async def __aexit__(self, *args: object) -> None:
        pass


def _make_job_detail_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    backend: object | None = None,
    actions_enabled: bool | None = None,
) -> TestClient:
    """A TestClient serving the job-detail page from stubbed reads.

    ``actions_enabled=None`` keeps the deployment default (the setting is
    absent from the env) - the exact configuration every fresh deployment
    ships with.
    """
    if actions_enabled is None:
        monkeypatch.delenv("TASKQ_ADMIN_ACTIONS_ENABLED", raising=False)
    else:
        monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", str(actions_enabled).lower())
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")

    pool = _JobDetailPool()
    pool.acquire = lambda **kwargs: _JobDetailAcquire(pool)  # type: ignore[method-assign]

    bundle = create_router(pool, backend=backend)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool; the admin router duck-types asyncpg.Pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


# ── B1: no backend on the bundle → the backend-mediated buttons must not render ─


def test_job_detail_without_backend_renders_no_backend_buttons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """backend=None (the shipped default): no Cancel/Retry forms at all.

    Every backend-mediated route answers 503 from that deployment, so a
    rendered button is a dead button - the templates must consult the
    bundle the render context carries and hide them.
    """
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.get(f"/jobs/{_JOB_ID}")
    assert resp.status_code == 200
    html = resp.text
    assert "Cancel Job" not in html, "a deployment with no backend rendered a dead Cancel button"
    assert "Retry Job" not in html, "a deployment with no backend rendered a dead Retry button"


def test_job_detail_with_backend_renders_the_buttons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: with a backend (and actions enabled) the buttons render."""
    from . import StubBackend

    client = _make_job_detail_app(monkeypatch, backend=StubBackend(), actions_enabled=True)
    resp = client.get(f"/jobs/{_JOB_ID}")
    assert resp.status_code == 200
    assert "Cancel Job" in resp.text


# ── B2: disabled actions → no forms, one banner, an actionable POST refusal ─


def test_job_detail_with_actions_disabled_renders_no_forms_and_one_banner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default settings: the deployment cannot act, so no action form
    renders and one banner says why (with the setting's name, so the
    operator knows the knob)."""
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.get(f"/jobs/{_JOB_ID}")
    assert resp.status_code == 200
    html = resp.text
    assert "Cancel Job" not in html
    assert "Retry Job" not in html
    assert _ACTIONS_BANNER in html
    assert html.count(_ACTIONS_BANNER) == 1, "the disabled-actions banner must render once"


def test_disabled_actions_post_is_refused_actionably_before_csrf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST to a mutation route on a disabled deployment: the refusal names
    the setting, and it wins over the CSRF check (a config state is safe
    to answer before any token validation; the token still guards when
    enabled)."""
    client = _make_job_detail_app(monkeypatch, backend=None)
    resp = client.post(f"/jobs/{_JOB_ID}/cancel", data={"reason": "nope"})
    assert resp.status_code == 403
    assert "TASKQ_ADMIN_ACTIONS_ENABLED" in resp.text, (
        f"the refusal must be the actionable config message, got: {resp.text[:200]}"
    )
    assert "CSRF" not in resp.text, "the enabled-check must run BEFORE the CSRF check"


def test_enabled_actions_post_still_requires_csrf(monkeypatch: pytest.MonkeyPatch) -> None:
    """The reorder must not weaken the token: with actions enabled, a POST
    without a token is still refused by the CSRF guard."""
    from . import StubBackend

    client = _make_job_detail_app(monkeypatch, backend=StubBackend(), actions_enabled=True)
    resp = client.post(f"/jobs/{_JOB_ID}/cancel", data={"reason": "nope"})
    assert resp.status_code == 403
    assert "CSRF" in resp.text


# ── B1: the shipped example deployments pass a backend ────────────────────


_EXAMPLES = (
    ("examples/app.py", "the embedded-shape trigger app"),
    ("examples/admin_app.py", "the sidecar-shape admin app"),
)


@pytest.mark.parametrize(("path", "role"), _EXAMPLES)
def test_example_deployments_pass_a_backend(path: str, role: str) -> None:
    """B1's wiring half: both shipped example deployments hand the admin
    router a Backend, so their action buttons WORK (the compose-stack e2e
    drives a live cancel through them)."""
    import inspect

    import examples.admin_app
    import examples.app

    module = examples.app if path.endswith("app.py") else examples.admin_app
    source = inspect.getsource(module)
    assert re.search(r"create_router\([^)]*backend=", source, re.DOTALL), (
        f"{role} ({path}) mounts the admin router without backend=: every "
        "backend-mediated action button on that deployment posts to a 503"
    )
