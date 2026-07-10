"""Private test-only worker entry point.

Invoked via ``sys.executable -m tests._worker_harness`` by integration
tests. Loads ``WorkerSettings`` from the environment and runs the
production ``_main`` bootstrap. The harness is pure glue — no signal
handling, no health-socket probing, no business logic.
"""

import asyncio
import sys

from taskq.settings import WorkerSettings
from taskq.worker.run import _main

if __name__ == "__main__":
    settings = WorkerSettings.load()
    try:
        with asyncio.Runner() as runner:
            sys.exit(runner.run(_main(settings)))
    except KeyboardInterrupt:
        sys.exit(0)
