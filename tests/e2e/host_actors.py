"""Host-importable actor registry for workgroup-supervised e2e workers.

The workgroup supervisor spawns children as ``python -m taskq worker
--actors module:attr`` on the HOST (workgroup.py ``_spawn_child``), where
the e2e package resolves as ``tests.e2e`` (repo root on PYTHONPATH), not
as ``e2e`` (container-only, PYTHONPATH=/app). ``worker_entry.py`` therefore
cannot be reused for host-spawned workers: its ``from e2e.actors import
...`` fails on the host. This module builds a registry from absolute
``tests.e2e.actors`` imports instead; the workgroup TOML written by
``test_workgroup.py`` points at ``tests.e2e.host_actors:ACTORS``.

DI constraint: the CLI worker path (cli.py ``worker``) calls
``worker_main`` WITHOUT a ``di_registry``, so the bootstrap's default
registry is used. It registers the worker's own pool as the LOOP-scope
``asyncpg.Pool`` provider (worker/_bootstrap.py), which covers every actor
that injects only ``pool``. Actors needing custom providers
(``enrich_order`` -> ``FakeHttpClient``) would fail ``validate()`` at
bootstrap and are deliberately omitted here: the workgroup e2e tests
supervision semantics (spawn, process, respawn), not DI coverage, which
test_di.py owns via the container entrypoint.
"""

from __future__ import annotations

from typing import Any

from taskq import ActorRef
from tests.e2e.actors import quick_result, send_welcome_email

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "send_welcome_email": send_welcome_email,
    "quick_result": quick_result,
}
