"""Unit tests for the three-state real-time badge and Redis health cache in the admin UI."""

from unittest.mock import AsyncMock

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin import create_router
from taskq.web.admin._factory import (  # pyright: ignore[reportPrivateUsage]  # Why: testing internal cache mechanics that have no other public surface.
    _redis_health_cache,
    get_realtime_mode,
)

from . import _StubPool

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_health_cache() -> None:
    """Reset the module-level Redis health cache before each test."""
    _redis_health_cache.ok = False
    _redis_health_cache.expires_at = 0.0


# ---------------------------------------------------------------------------
# no redis_client → always polling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_redis_client_returns_polling() -> None:
    """get_realtime_mode(None) returns ('polling', 'polling mode')."""
    result = await get_realtime_mode(None)
    assert result == ("polling", "polling mode")


# ---------------------------------------------------------------------------
# redis_client present, ping succeeds → realtime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redis_ping_success_returns_realtime() -> None:
    """Successful ping returns ('realtime', 'real-time mode')."""
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(return_value=True)
    result = await get_realtime_mode(mock_client)
    assert result == ("realtime", "real-time mode")


# ---------------------------------------------------------------------------
# redis_client present, ping raises ConnectionError → degraded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redis_ping_connection_error_returns_degraded() -> None:
    """ping raising ConnectionError returns ('polling-degraded', ...)."""
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(side_effect=ConnectionError("refused"))
    result = await get_realtime_mode(mock_client)
    assert result == ("polling-degraded", "polling mode (Redis unavailable)")


# ---------------------------------------------------------------------------
# ping raises asyncio.TimeoutError → degraded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redis_ping_timeout_returns_degraded() -> None:
    """ping raising asyncio.TimeoutError returns ('polling-degraded', ...)."""
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(side_effect=TimeoutError())
    result = await get_realtime_mode(mock_client)
    assert result == ("polling-degraded", "polling mode (Redis unavailable)")


# ---------------------------------------------------------------------------
# cache hit — ping called exactly once across two calls within TTL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_hit_ping_called_once() -> None:
    """Two calls within the cache TTL invoke ping exactly once."""
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(return_value=True)

    first = await get_realtime_mode(mock_client)
    second = await get_realtime_mode(mock_client)

    assert first == ("realtime", "real-time mode")
    assert second == ("realtime", "real-time mode")
    mock_client.ping.assert_called_once()


# ---------------------------------------------------------------------------
# cache expiry — ping called twice when expires_at is wound back
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_expiry_ping_called_twice() -> None:
    """Expiring the cache between two calls causes ping to be called twice."""
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(return_value=True)

    # First call populates the cache.
    await get_realtime_mode(mock_client)
    assert mock_client.ping.call_count == 1

    # Forcibly expire the cache by winding expires_at to the past.
    _redis_health_cache.expires_at = 0.0

    # Second call should re-ping because the cache is stale.
    await get_realtime_mode(mock_client)
    assert mock_client.ping.call_count == 2


# ---------------------------------------------------------------------------
# poll_interval_ms in rendered template reflects settings
# ---------------------------------------------------------------------------


def test_poll_interval_ms_in_template(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """admin_ui_polling_interval_seconds=5.0 → poll_interval_ms=5000 in the Jinja2 env."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS", "5.0")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    assert env.globals["poll_interval_ms"] == 5000  # pyright: ignore[reportArgumentType]  # Why: Jinja2 globals dict is typed narrowly; int is a valid template global.


# ---------------------------------------------------------------------------
# meta refresh present/absent based on realtime_mode
# ---------------------------------------------------------------------------


def _render_base_with_mode(
    monkeypatch: pytest.MonkeyPatch,
    pool: _StubPool,
    realtime_mode: str,
    mode_label: str,
) -> str:
    """Render _base.html with the given realtime_mode and mode_label."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    return env.get_template("_base.html").render(realtime_mode=realtime_mode, mode_label=mode_label)


def test_meta_refresh_present_in_polling_mode(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """realtime_mode='polling' → <meta http-equiv='refresh'> is present."""
    html = _render_base_with_mode(
        monkeypatch, stub_pool, realtime_mode="polling", mode_label="polling mode"
    )
    assert '<meta http-equiv="refresh"' in html


def test_meta_refresh_absent_in_realtime_mode(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """realtime_mode='realtime' → <meta http-equiv='refresh'> is absent."""
    html = _render_base_with_mode(
        monkeypatch, stub_pool, realtime_mode="realtime", mode_label="real-time mode"
    )
    assert '<meta http-equiv="refresh"' not in html


def test_meta_refresh_present_in_polling_degraded_mode(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """realtime_mode='polling-degraded' → <meta http-equiv='refresh'> is present."""
    html = _render_base_with_mode(
        monkeypatch,
        stub_pool,
        realtime_mode="polling-degraded",
        mode_label="polling mode (Redis unavailable)",
    )
    assert '<meta http-equiv="refresh"' in html
