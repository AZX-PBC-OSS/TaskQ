"""Private test-only worker entry point.

Invoked via ``sys.executable -m tests._worker_harness`` by integration
tests. Loads ``WorkerSettings`` from the environment and runs the
production ``_main`` bootstrap. The harness is pure glue - no signal
handling, no health-socket probing, no business logic.

The worker is driven through ``run_worker_to_exit`` so the spawn path
shares the entrypoint teardown guard: the loop's close restores SIG_DFL,
and a stop signal landing in the teardown window must not replace the
worker's exit status with ``-15``.
"""

import sys

from taskq.settings import WorkerSettings
from taskq.worker.run import _main, run_worker_to_exit

if __name__ == "__main__":
    settings = WorkerSettings.load()
    try:
        sys.exit(run_worker_to_exit(_main(settings)))
    except KeyboardInterrupt:
        sys.exit(0)
