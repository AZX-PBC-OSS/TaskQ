"""The standalone progress router must fail closed when served without auth.

``taskq.web.admin.create_router`` raises ``RuntimeError`` when
``auth_dependency`` is ``None`` outside a dev environment (unless the
operator explicitly suppresses the check). ``taskq.web.progress.create_router``
exposes the same shape of surface — per-job status and progress state, plus
an SSE endpoint that holds a Redis subscription per anonymous connection —
and is documented for standalone mounting, so it must apply the same
fail-closed gate: no auth dependency outside a dev environment is a startup
error, never a silently open router.
"""

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("sse_starlette")

from taskq.web.progress import create_router


class _StubPool:
    """Duck-typed stand-in for asyncpg.Pool — the factory only stores it."""


def test_create_router_raises_without_auth_outside_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """auth_dependency=None in a non-dev environment must raise, as the
    admin router factory already does under identical inputs."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")

    with pytest.raises(RuntimeError, match="auth_dependency"):
        create_router(
            _StubPool(),  # pyright: ignore[reportArgumentType]  # Why: test duck-type.
            None,
            auth_dependency=None,
        )


def test_create_router_allows_no_auth_in_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate is environment-scoped: a dev-labeled process may serve the
    router without auth (matching the admin factory's dev carve-out)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    router = create_router(
        _StubPool(),  # pyright: ignore[reportArgumentType]  # Why: test duck-type.
        None,
        auth_dependency=None,
    )
    assert router is not None


def test_create_router_warns_when_serving_without_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unauthenticated serving always logs a warning naming what
    suppressed the fail-closed check — the documented shipped behavior
    (progress.md), and the operator's signal that an unauthenticated
    SSE/state surface is live."""
    import structlog

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    with structlog.testing.capture_logs() as logs:
        create_router(
            _StubPool(),  # pyright: ignore[reportArgumentType]  # Why: test duck-type.
            None,
            auth_dependency=None,
        )

    entries = [log for log in logs if log.get("event") == "progress-router-no-auth"]
    assert len(entries) == 1
    # The suppression reason names the dev carve-out — the same
    # detail-embedded shape the admin factory's warning uses.
    assert "dev environment" in entries[0]["detail"]


def test_create_router_with_auth_dependency_succeeds_outside_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supplying an auth dependency satisfies the gate in any environment."""

    async def _auth() -> None: ...

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")

    router = create_router(
        _StubPool(),  # pyright: ignore[reportArgumentType]  # Why: test duck-type.
        None,
        auth_dependency=_auth,
    )
    assert router is not None
