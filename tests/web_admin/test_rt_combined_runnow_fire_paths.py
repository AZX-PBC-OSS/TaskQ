"""Cross-PR attack: the admin run-now fire path vs the singleton contract.

The campaign fixed singleton parity for two of the three fire paths:
client enqueues stamp ``metadata["singleton"]`` at build time
(``client/_args.py``), and the cron tick carries it via
``ActorFirePolicy`` (``worker/cron_loop.py``, the parity comment in
``worker/_bootstrap.py``). The admin run-now handler (#411/#429) builds
its ``EnqueueArgs`` directly from the stored ``actor_config`` row and
stamps ONLY ``cron_schedule_id``, so a fire pressed from the admin UI
silently bypasses the ``jobs_singleton_uniq`` partial index and the
singleton preflight in BOTH directions:

* a run-now job lands WITHOUT the flag, so while it lives the actor can
  take other concurrent jobs no singleton enqueue will consider it a
  blocker either (the preflight keys on the ROW's metadata);
* when a live singleton job DOES exist, the backend raises
  ``SingletonCollisionError`` -- a ``BackpressureError`` SUBCLASS -- so
  the run-now handler's ``except BackpressureError`` answers it with a
  redirect that says the actor is "at max_pending cap": a factually
  wrong reason shown to the operator.

These pins demand the same parity for the third fire path the other two
already have, and demand the collision refusal name its real reason.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq._ids import new_uuid
from taskq.exceptions import SingletonCollisionError
from taskq.web.admin import create_router, setup_admin_state

from . import StubBackend, StubConnection, StubPool, StubRecord, _stub_job_row

pytestmark = [pytest.mark.fastapi]


class _OneShotAcquire:
    async def __aenter__(self) -> StubConnection:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass

    def __init__(self, conn: StubConnection) -> None:
        self._conn = conn


class _ScriptedPool(StubPool):
    """StubPool handing out one scripted connection."""

    def __init__(self, conn: StubConnection) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> _OneShotAcquire:  # type: ignore[override]  # Why: the stub narrows the context type; asyncpg's real acquire returns a wider context.
        return _OneShotAcquire(self._conn)


class _FirePolicy:
    """Structural stand-in for ``worker/cron_loop.ActorFirePolicy``."""

    def __init__(self, singleton: bool, max_pending: int | None = None) -> None:
        self.singleton = singleton
        self.max_pending = max_pending


class _CollisionBackend(StubBackend):
    """Backend whose enqueue raises the singleton collision the Layer-1
    preflight / partial index produce."""

    def __init__(self) -> None:
        super().__init__(job_row=_stub_job_row(new_uuid()))
        self.enqueue_calls: list[Any] = []

    async def enqueue(self, args: Any) -> Any:
        self.enqueue_calls.append(args)
        raise SingletonCollisionError(
            actor=args.actor,
            blocking_job_id=UUID("00000000-0000-0000-0000-000000000001"),
        )


def _make_app(pool: Any, **kwargs: Any) -> TestClient:
    bundle = create_router(pool, **kwargs)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)  # type: ignore[arg-type]


def _get_csrf_token(client: TestClient) -> str:
    client.get("/queues")
    return client.cookies.get("taskq_csrf_token") or ""


def _run_now_rows() -> tuple[StubRecord, StubRecord]:
    schedule_row = StubRecord(actor="cleanup", payload_factory=None, enabled=True, metadata={})
    actor_config_row = StubRecord(
        queue="default",
        max_attempts=3,
        retry_kind="transient",
        retry_base=None,
        retry_cap=None,
        retry_backoff=None,
        retry_jitter=None,
        max_pending=None,
    )
    return schedule_row, actor_config_row


def test_run_now_on_singleton_actor_stamps_the_singleton_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run-now fire for an actor the registry declares singleton carries
    ``metadata["singleton"]``, exactly as the client and cron fire paths
    do: without the flag the row is invisible to the ``jobs_singleton_uniq``
    index and to every later singleton preflight for the actor, and the
    "at most one active job" contract is silently gone from the admin
    surface."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    schedule_row, actor_config_row = _run_now_rows()
    conn = StubConnection()
    conn.fetchrow = _scripted_fetchrow([schedule_row, actor_config_row])  # type: ignore[method-assign]
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    client = _make_app(
        _ScriptedPool(conn),
        backend=backend,
        actor_fire_policies={"cleanup": _FirePolicy(singleton=True)},
    )
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.enqueue_calls) == 1
    assert backend.enqueue_calls[0].metadata.get("singleton") is True
    # Provenance parity is unchanged.
    assert backend.enqueue_calls[0].metadata.get("cron_schedule_id") == str(sid)


def test_run_now_singleton_collision_names_the_real_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a live singleton job blocks the fire, the redirect names the
    singleton collision, not "at max_pending cap": SingletonCollisionError
    is a BackpressureError subclass, and the catch-all answered it with a
    reason that is factually wrong."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    schedule_row, actor_config_row = _run_now_rows()
    conn = StubConnection()
    conn.fetchrow = _scripted_fetchrow([schedule_row, actor_config_row])  # type: ignore[method-assign]
    backend = _CollisionBackend()
    client = _make_app(
        _ScriptedPool(conn),
        backend=backend,
        actor_fire_policies={"cleanup": _FirePolicy(singleton=True)},
    )
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    location = resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]
    assert "error=" in location
    assert "max_pending" not in location
    assert "singleton" in location
    # The refusal wrote nothing: the collision refused the enqueue.
    assert len(backend.enqueue_calls) == 1


def test_run_now_non_singleton_actor_stamps_no_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Over-refusal guard: an actor whose fire policy does not declare
    singleton enqueues exactly as before (no invented flag)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    schedule_row, actor_config_row = _run_now_rows()
    conn = StubConnection()
    conn.fetchrow = _scripted_fetchrow([schedule_row, actor_config_row])  # type: ignore[method-assign]
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    client = _make_app(
        _ScriptedPool(conn),
        backend=backend,
        actor_fire_policies={"cleanup": _FirePolicy(singleton=False)},
    )
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.enqueue_calls) == 1
    assert "singleton" not in backend.enqueue_calls[0].metadata


def test_run_now_unknown_policy_host_still_enqueues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A host that supplies no fire-policy mapping (a standalone admin
    process, the only shape the docs could serve before) keeps the
    documented residual: no flag, no refusal -- disclosed, not silent."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    schedule_row, actor_config_row = _run_now_rows()
    conn = StubConnection()
    conn.fetchrow = _scripted_fetchrow([schedule_row, actor_config_row])  # type: ignore[method-assign]
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    client = _make_app(_ScriptedPool(conn), backend=backend)
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.enqueue_calls) == 1
    assert "singleton" not in backend.enqueue_calls[0].metadata


def _scripted_fetchrow(results: list[StubRecord | None]) -> Any:
    async def _fetchrow(query: str, *args: object) -> StubRecord | None:
        if not results:
            return None
        return results.pop(0)

    return _fetchrow
