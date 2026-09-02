"""``build_worker_connections`` accepts an explicit DSN, not only ``TASKQ_PG_DSN``.

The gap this closes is a connection-budget one. A consumer that already knows
where TaskQ's tables live - because its own settings say so - could not adopt
``build_worker_connections`` without ALSO exporting ``TASKQ_PG_DSN`` to the
same value, duplicating one fact across two config systems. So it built one
``make_pg_pool_factory`` by hand and passed that same factory object to all
three pool roles. TaskQ resolves each role independently, so every role got a
full ``pool_max``-sized pool: **5x pool_max + 3** connections per replica
instead of the ``dispatcher=4`` / ``heartbeat=4`` / ``worker=derived`` budget
``build_worker_connections`` sizes from settings. That difference chose a
Postgres SKU.

Per-role sizing is not something ``WorkerConnections`` can carry: it holds
*factories*, and a factory's sizing is closed over inside it - TaskQ cannot
resize the pool a caller-supplied factory returns. The sizing lives where the
factories are built, so the fix belongs here.
"""

from __future__ import annotations

from typing import Any

import pytest

from taskq import auth as auth_mod
from taskq.testing.settings import make_integration_settings

_SETTINGS_DSN = "postgresql://taskq:taskq@settings-host:5432/taskq"
_EXPLICIT_DIRECT = "postgresql://taskq:taskq@explicit-direct:5432/appdb"
_EXPLICIT_POOLED = "postgresql://taskq:taskq@explicit-pooled:6432/appdb"


class _Provider:
    async def get_pg_credential(self) -> Any:  # pragma: no cover - never called
        raise AssertionError


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    calls: list[tuple[str, dict[str, Any]]] = []

    def _fake_pool_factory(dsn: str, _provider: object, **kwargs: Any) -> object:
        calls.append((dsn, kwargs))
        return lambda: None

    def _fake_conn_factory(dsn: str, _provider: object, **kwargs: Any) -> object:
        calls.append((dsn, kwargs))
        return lambda: None

    monkeypatch.setattr(auth_mod, "make_pg_pool_factory", _fake_pool_factory)
    monkeypatch.setattr(auth_mod, "make_dedicated_conn_factory", _fake_conn_factory)
    return calls


def test_explicit_dsn_overrides_settings_for_every_pg_role(
    captured: list[tuple[str, dict[str, Any]]],
) -> None:
    """One ``pg_dsn`` argument redirects all five Postgres roles."""
    settings = make_integration_settings(_SETTINGS_DSN)

    auth_mod.build_worker_connections(
        settings,
        pg_provider=_Provider(),  # type: ignore[arg-type]  # Why: structural Protocol; the stub is never invoked.
        pg_dsn=_EXPLICIT_DIRECT,
    )

    assert captured, "no factories were built"
    assert {dsn for dsn, _ in captured} == {_EXPLICIT_DIRECT}


def test_explicit_dsn_keeps_the_settings_derived_per_role_sizing(
    captured: list[tuple[str, dict[str, Any]]],
) -> None:
    """The point of the override: a caller's DSN, TaskQ's connection budget.

    Sizing still comes from ``WorkerSettings`` — that is exactly what a
    hand-rolled single factory for all three roles throws away.
    """
    settings = make_integration_settings(
        _SETTINGS_DSN,
        DISPATCHER_POOL_SIZE="4",
        HEARTBEAT_POOL_SIZE="4",
    )

    auth_mod.build_worker_connections(
        settings,
        pg_provider=_Provider(),  # type: ignore[arg-type]  # Why: structural Protocol; the stub is never invoked.
        pg_dsn=_EXPLICIT_DIRECT,
    )

    sizes = sorted(kwargs["max_size"] for _dsn, kwargs in captured if "max_size" in kwargs)
    # worker_pool_size is derived from concurrency, not a literal — read it off
    # settings so the assertion tracks the derivation rather than pinning it.
    assert sizes == sorted([4, 4, settings.worker_pool_size])


def test_direct_and_pooled_can_be_overridden_independently(
    captured: list[tuple[str, dict[str, Any]]],
) -> None:
    """A pgbouncer topology keeps its split when the DSN comes from the caller."""
    settings = make_integration_settings(
        _SETTINGS_DSN, DISPATCHER_POOL_SIZE="4", HEARTBEAT_POOL_SIZE="4"
    )
    worker_size = settings.worker_pool_size
    assert worker_size not in (4, None), "sizing must differ from the direct roles to be told apart"

    auth_mod.build_worker_connections(
        settings,
        pg_provider=_Provider(),  # type: ignore[arg-type]  # Why: structural Protocol; the stub is never invoked.
        pg_dsn_direct=_EXPLICIT_DIRECT,
        pg_dsn_pooled=_EXPLICIT_POOLED,
    )

    pooled = [dsn for dsn, kwargs in captured if kwargs.get("max_size") == worker_size]
    assert pooled == [_EXPLICIT_POOLED]
    assert all(
        dsn == _EXPLICIT_DIRECT for dsn, kwargs in captured if kwargs.get("max_size") != worker_size
    )


def test_pg_dsn_and_the_split_form_are_mutually_exclusive() -> None:
    settings = make_integration_settings(_SETTINGS_DSN)

    with pytest.raises(ValueError, match="pg_dsn"):
        auth_mod.build_worker_connections(
            settings,
            pg_provider=_Provider(),  # type: ignore[arg-type]  # Why: the guard fires before the provider is used.
            pg_dsn=_EXPLICIT_DIRECT,
            pg_dsn_direct=_EXPLICIT_DIRECT,
        )


def test_settings_dsn_is_still_the_default(
    captured: list[tuple[str, dict[str, Any]]],
) -> None:
    """No override means no behaviour change for existing deployments."""
    settings = make_integration_settings(_SETTINGS_DSN)

    auth_mod.build_worker_connections(
        settings,
        pg_provider=_Provider(),  # type: ignore[arg-type]  # Why: structural Protocol; the stub is never invoked.
    )

    assert {dsn for dsn, _ in captured} == {str(settings.resolved_pg_dsn_direct)}
