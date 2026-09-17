"""The PROCESS/THREAD/LOOP scope chain a worker bootstraps, for tests.

Tests that drive the DI-resolved dispatch path build the same three
scope containers the worker's bootstrap does, over a registry, wired
through :func:`taskq._di.scopes.make_resolver` — the production resolver,
so nested resolution from a LOOP factory reaches PROCESS and THREAD
providers exactly as it does in a worker. One builder here keeps every
suite on that shape.
"""

from __future__ import annotations

from typing import Any

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq.settings import WorkerSettings

__all__ = [
    "BootstrappedScopes",
    "bootstrap_scopes",
    "make_scopes",
    "scope_settings",
]


def scope_settings(**overrides: object) -> WorkerSettings:
    """The minimal ``WorkerSettings`` a scope bootstrap needs: a DSN nothing
    connects to and the lease/heartbeat pair its validator requires."""
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            **overrides,
        },
    )


def make_scopes(
    registry: ProviderRegistry,
    *,
    factory_timeout: float | None = None,
) -> tuple[ProcessScope, ThreadScope, LoopScope]:
    """The three scope containers over *registry*, each resolving through
    the shared container map (the worker bootstrap's own wiring)."""
    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)
    process_scope = ProcessScope(resolver=resolver, factory_timeout=factory_timeout)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver, factory_timeout=factory_timeout)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver, factory_timeout=factory_timeout)
    scope_containers[Scope.LOOP] = loop_scope
    return process_scope, thread_scope, loop_scope


async def bootstrap_scopes(
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
    settings: WorkerSettings | None = None,
) -> None:
    """Bootstrap the chain in the worker's order: PROCESS, then THREAD,
    then LOOP. *settings* defaults to :func:`scope_settings`."""
    await process_scope.bootstrap(registry, settings if settings is not None else scope_settings())
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)


class BootstrappedScopes:
    """A validated registry's scope chain, bootstrapped on entry and shut
    down LOOP-first on exit — what a test that dispatches through the
    worker's DI path needs around each dispatch."""

    def __init__(
        self,
        registry: ProviderRegistry,
        *,
        settings: WorkerSettings | None = None,
        validate: bool = True,
    ) -> None:
        self.registry = registry
        self._settings = settings
        self._validate = validate

    async def __aenter__(self) -> BootstrappedScopes:
        if self._validate:
            self.registry.validate()
        self.process_scope, self.thread_scope, self.loop_scope = make_scopes(self.registry)
        await bootstrap_scopes(
            self.registry,
            self.process_scope,
            self.thread_scope,
            self.loop_scope,
            self._settings,
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()
