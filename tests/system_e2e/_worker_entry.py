"""Worker subprocess entry point for the system-e2e tier.

Spawned as ``sys.executable -m tests.system_e2e._worker_entry`` with the
worker's ``TASKQ_*`` environment set by the spawning test. Loads
``WorkerSettings`` from that environment exactly the way the production
entry points do and runs the production ``_main`` bootstrap against the
shared actor registry. Pure glue: no signal handling, no business logic,
no per-test seams - the scenario injects its chaos from the outside
(signals, kills, SQL, the broker).
"""

import asyncio
import sys

from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.system_e2e.actors import ACTORS

if __name__ == "__main__":
    settings = WorkerSettings.load()
    try:
        with asyncio.Runner() as runner:
            sys.exit(runner.run(_main(settings, actor_registry=ACTORS)))
    except KeyboardInterrupt:
        sys.exit(0)
