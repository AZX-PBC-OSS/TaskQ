"""Tests for backend delegation in admin route handlers.

Verifies that job_cancel, job_retry, and schedule_run_now delegate to the
Backend instead of using inline SQL, and that routes return 503 when no
backend is configured.
"""

from collections.abc import Callable
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.backend._protocol import JobId

from . import StubBackend, _stub_job_row


def _get_csrf_token(client: Any) -> str:
    """GET the queues page to set the CSRF cookie, then return the token value."""
    client.get("/queues")
    return client.cookies.get("taskq_csrf_token", "")


# ── 503 when no backend configured ──────────────────────────────────────


def test_cancel_returns_503_without_backend(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """POST /jobs/{id}/cancel returns 503 when no backend is configured."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    jid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/cancel", data={"csrf_token": token})
    assert resp.status_code == 503  # pyright: ignore[reportUnknownMemberType]


def test_retry_returns_503_without_backend(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """POST /jobs/{id}/retry returns 503 when no backend is configured."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    jid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token})
    assert resp.status_code == 503  # pyright: ignore[reportUnknownMemberType]


def test_schedule_run_returns_503_without_backend(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """POST /schedules/{id}/run returns 503 when no backend is configured."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token})
    assert resp.status_code == 503  # pyright: ignore[reportUnknownMemberType]


# ── Cancel delegates to backend.write_cancel_request ────────────────────


def test_cancel_delegates_to_backend(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/cancel calls backend.get and backend.write_cancel_request."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    job_row = _stub_job_row(jid, status="pending")
    backend = StubBackend(job_row=job_row)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/cancel", data={"csrf_token": token}, follow_redirects=False)

    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.cancel_calls) == 1
    assert backend.cancel_calls[0][0] == JobId(jid)


def test_cancel_returns_404_when_job_not_found(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/cancel returns 404 when backend.get returns None."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    backend = StubBackend(job_row=None)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/cancel", data={"csrf_token": token})
    assert resp.status_code == 404  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.cancel_calls) == 0


def test_cancel_returns_409_for_terminal_job(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/cancel returns 409 when job is in a terminal state."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    job_row = _stub_job_row(jid, status="succeeded")
    backend = StubBackend(job_row=job_row)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/cancel", data={"csrf_token": token})
    assert resp.status_code == 409  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.cancel_calls) == 0


def test_cancel_passes_reason_to_backend(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/cancel?reason=... passes the reason to the backend."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    job_row = _stub_job_row(jid, status="running")
    backend = StubBackend(job_row=job_row)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(
        f"/jobs/{jid}/cancel",
        params={"reason": "duplicate"},
        data={"csrf_token": token},
        follow_redirects=False,
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.cancel_calls) == 1
    assert backend.cancel_calls[0][1] == "duplicate"


# ── Retry delegates to backend.retry_job ────────────────────────────────


def test_retry_delegates_to_backend(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/retry calls backend.get and backend.retry_job."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    job_row = _stub_job_row(jid, status="failed")
    backend = StubBackend(job_row=job_row)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.retry_calls) == 1
    assert backend.retry_calls[0] == JobId(jid)


def test_retry_returns_404_when_job_not_found(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/retry returns 404 when backend.get returns None."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    backend = StubBackend(job_row=None)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token})
    assert resp.status_code == 404  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.retry_calls) == 0


def test_retry_returns_409_for_non_retryable_job(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/retry returns 409 when job is not in a retryable state."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    job_row = _stub_job_row(jid, status="pending")
    backend = StubBackend(job_row=job_row)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token})
    assert resp.status_code == 409  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.retry_calls) == 0


def test_retry_succeeds_for_crashed_job(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/retry succeeds for a crashed job."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    job_row = _stub_job_row(jid, status="crashed")
    backend = StubBackend(job_row=job_row)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.retry_calls) == 1


def test_retry_succeeds_for_cancelled_job(
    monkeypatch: pytest.MonkeyPatch,
    make_app_with_backend: Callable[..., Any],
) -> None:
    """POST /jobs/{id}/retry succeeds for a cancelled job."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = uuid4()
    job_row = _stub_job_row(jid, status="cancelled")
    backend = StubBackend(job_row=job_row)
    client, backend = make_app_with_backend(backend=backend)

    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.retry_calls) == 1
