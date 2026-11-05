"""DocShelf's worker entrypoint — the registry the ``taskq worker`` CLI
loads (``--actors examples.docshelf.worker_entry:ACTORS``).

Importing :mod:`examples.docshelf.app`
compiles the workflow definitions into THIS process (the D1 registry the
fleet's execution door resolves bodies from) and projects the workflows'
FULL cohort surface into ``actor_config`` (the boot projection) — the
wired nodes, the map attachments, the chains, and the route arms'
effective placements (the projection resolves the arms'
``arm.actor or node.actor, arm.queue or node.map_queue``; the app
carries no manual cohort registration). A worker that did not import
the app can never claim a flow row, by design.
"""

from typing import Any

from examples.docshelf.app import docshelf_nightly_tick
from taskq import ActorRef

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "docshelf-nightly-tick": docshelf_nightly_tick,
}
