"""Worker subprocess entry point for the system-e2e tier.

Spawned as ``sys.executable -m tests.system_e2e._worker_entry`` with the
worker's ``TASKQ_*`` environment set by the spawning test. Loads
``WorkerSettings`` from that environment exactly the way the production
entry points do and runs the production ``_main`` bootstrap against
the shared actor registry. Pure glue: deliberately NO logging setup
(the harness pipes stdout/stderr and reads them only at exit - a
configured, chatty worker would fill the 64K pipe buffer and block
mid-write, the not-settled-at-load shape), no business logic, no
per-test seams - the scenario injects its chaos from the outside
(signals, kills, SQL, the broker). The post-drain SIGTERM guard
mirrors worker_main's (see its comment): the loop close restores
SIG_DFL, and the harness's stop signal must not erase a drained
pod's verdict.
"""

import asyncio
import contextlib
import signal
import sys

from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.system_e2e.actors import ACTORS

if __name__ == "__main__":
    settings = WorkerSettings.load()
    try:
        with asyncio.Runner() as runner:
            code = runner.run(_main(settings, actor_registry=ACTORS))
        # Same post-drain guard worker_main carries (see its comment): the
        # closed loop restored SIG_DFL, and the harness's graceful_stop
        # signals a second time - a pod that drained cleanly must exit
        # with the drain's verdict, not die by the redundant signal.
        with contextlib.suppress(ValueError):  # Why: not the main thread -> no window to guard.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        sys.exit(code)
    except KeyboardInterrupt:
        sys.exit(0)
