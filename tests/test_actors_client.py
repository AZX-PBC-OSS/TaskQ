"""Tests for ActorsClient — the pool-wrapping facade over actor_config_ops.

These tests use a fake pool to verify the delegation wiring without
requiring real Postgres (the ops functions themselves are integration-tested
in test_actor_deregistration.py and test_actor_config_ops.py).
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from taskq.actor_config_ops import (
    ActorConfigRow,
    DeregisterResult,
)
from taskq.exceptions import ActorNotFoundError

pytestmark = [pytest.mark.asyncio]


class _FakeConn:
    """Fake connection — just needs to be passable to the ops functions."""


class _FakePool:
    """Minimal pool that yields a fake connection via async context manager."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def acquire(self) -> Any:
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=self._conn)
        cm.__aexit__ = AsyncMock(return_value=None)
        return cm


async def test_actors_client_list_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.client._actors import ActorsClient

    conn = _FakeConn()
    pool = _FakePool(conn)
    client = ActorsClient(pool, schema="test_schema")

    mock_result = [
        ActorConfigRow(
            actor="a",
            max_concurrent=1,
            max_pending=None,
            queue="q",
            result_ttl=None,
            metadata={},
            updated_at="2026-01-01",
        )
    ]
    import taskq.client._actors as actors_mod

    mock_list = AsyncMock(return_value=mock_result)
    monkeypatch.setattr(actors_mod, "list_actor_configs", mock_list)
    result = await client.list()
    assert result == mock_result
    mock_list.assert_called_once_with(conn, schema="test_schema")


async def test_actors_client_deregister_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.client._actors import ActorsClient

    conn = _FakeConn()
    pool = _FakePool(conn)
    client = ActorsClient(pool, schema="test_schema")

    expected = DeregisterResult(
        actor="test-actor",
        queue="q",
        actor_config_deleted=True,
        schedules_disabled=0,
        jobs_cancelled=0,
        terminal_jobs_remaining=0,
        queue_purged=False,
    )

    import taskq.client._actors as actors_mod

    mock_deregister = AsyncMock(return_value=expected)
    monkeypatch.setattr(actors_mod, "deregister_actor", mock_deregister)
    result = await client.deregister("test-actor", force=True, purge_queue=True)
    assert result == expected
    mock_deregister.assert_called_once_with(
        conn, "test-actor", force=True, purge_queue=True, schema="test_schema"
    )


async def test_actors_client_get_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.client._actors import ActorsClient

    conn = _FakeConn()
    pool = _FakePool(conn)
    client = ActorsClient(pool, schema="test_schema")

    expected = ActorConfigRow(
        actor="a",
        max_concurrent=1,
        max_pending=None,
        queue="q",
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01",
    )

    import taskq.client._actors as actors_mod

    mock_get = AsyncMock(return_value=expected)
    monkeypatch.setattr(actors_mod, "get_actor_config", mock_get)
    result = await client.get("a")
    assert result == expected
    mock_get.assert_called_once_with(conn, "a", schema="test_schema")


async def test_actors_client_set_capacity_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.client._actors import ActorsClient

    conn = _FakeConn()
    pool = _FakePool(conn)
    client = ActorsClient(pool, schema="test_schema")

    expected = ActorConfigRow(
        actor="a",
        max_concurrent=10,
        max_pending=None,
        queue="q",
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01",
    )

    import taskq.client._actors as actors_mod

    mock_set_capacity = AsyncMock(return_value=expected)
    monkeypatch.setattr(actors_mod, "set_actor_config_capacity", mock_set_capacity)
    result = await client.set_capacity("a", max_concurrent=10)
    assert result == expected
    mock_set_capacity.assert_called_once_with(
        conn,
        "a",
        max_concurrent=10,
        max_pending=actors_mod.UNSET,
        result_ttl=actors_mod.UNSET,
        schema="test_schema",
    )


async def test_actors_client_deregister_propagates_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exceptions from the ops function must propagate through the pool-wrapper —
    not be silently swallowed. This is a lifecycle concern, not a delegation one."""
    from taskq.client._actors import ActorsClient

    conn = _FakeConn()
    pool = _FakePool(conn)
    client = ActorsClient(pool, schema="test_schema")

    import taskq.client._actors as actors_mod

    monkeypatch.setattr(
        actors_mod,
        "deregister_actor",
        AsyncMock(side_effect=ActorNotFoundError("bad-actor")),
    )
    with pytest.raises(ActorNotFoundError):
        await client.deregister("bad-actor")
