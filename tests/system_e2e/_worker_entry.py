"""Worker subprocess entry point for the system-e2e tier.

Spawned as ``sys.executable -m tests.system_e2e._worker_entry`` with the
worker's ``TASKQ_*`` environment set by the spawning test. Loads
``WorkerSettings`` from that environment exactly the way the production
entry points do and runs the production ``_main`` bootstrap against
the shared actor registry. Pure glue: deliberately no logging setup of
its own (the production bootstrap configures the log surface it needs;
the harness tees the child's stderr continuously and sends its stdout
to DEVNULL - a spawn path that leaves a pipe undrained is the wedge
class the harness exists to prevent), no business logic, no
per-test seams - the scenario injects its chaos from the outside
(signals, kills, SQL, the broker).

``run_worker_to_exit`` carries the post-drain SIGTERM guard every
dedicated worker process exit needs: closing the loop restores SIG_DFL,
and the harness's stop signal must not erase a drained pod's verdict.
"""

import faulthandler
import signal
import sys

from taskq.settings import WorkerSettings
from taskq.worker.run import _main, run_worker_to_exit
from tests.system_e2e._kill_actors import ACTORS as KILL9_ACTORS
from tests.system_e2e.actors import ACTORS

#: The full worker-side registry: the graceful scenarios' actors plus the
#: kill9 campaign's (the kill seams live in ``_kill_entry``, not here -
#: this entry is always seam-free).
REGISTRY: dict[str, object] = {**ACTORS, **KILL9_ACTORS}

if __name__ == "__main__":
    # SIGUSR2 dumps every thread's traceback to stderr and continues: the
    # forensic a wedged-loop scenario needs (the harness pipes stderr and
    # reads it at exit).
    faulthandler.register(signal.SIGUSR2, all_threads=True)
    settings = WorkerSettings.load()
    try:
        sys.exit(run_worker_to_exit(_main(settings, actor_registry=REGISTRY)))
    except KeyboardInterrupt:
        sys.exit(0)
