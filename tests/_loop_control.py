"""The compatibility facade over the lifecycle registry's managed-loop
adapter. The converted drivers import this module; the graceful stop,
the two-phase bound, the caller drain, and the body > loop > drain
precedence live in the registry."""

from __future__ import annotations

import asyncio

from tests._lifecycle import (
    GRACE_SECONDS,  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: the drivers' grace constant keeps its import site.
    LifecycleScope,  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: re-exported for the pins.
    ManagedLoop,  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: re-exported for the pins.
    ScopeKind,  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: re-exported for the pins.
    install_lease_factory,
    managed_loop,  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: the facade is the drivers' import site.
)


def install_parent_link_factory(loop: asyncio.AbstractEventLoop) -> None:
    """The facade's install: the registry's lease factory on the loop."""
    install_lease_factory(loop)
