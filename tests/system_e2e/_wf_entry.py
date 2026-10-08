"""The workflow-capable worker subprocess entry point for the system-e2e
tier (T15).

Same shape as :mod:`tests.system_e2e._worker_entry` (the production
bootstrap, env-configured like a pod, the health socket as readiness)
with ONE addition: the march flows' app module is imported BEFORE the
bootstrap, so the process's registry holds the apps and the boot stamps
``workflow_execution: true`` into the workers row's metadata (the
dispatch fence's capability leg) and projects the (actor, queue)
cohorts (the F3 law's call site).

The kill seams stay in ``_kill_entry`` — this entry is always seam-free,
the same contract the vanilla entry keeps.
"""

import asyncio
import contextlib
import faulthandler
import signal
import sys

from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.system_e2e import (
    _wf_app as _wf_app_module,  # Why: the import IS the capability — the app registry observes it, the boot's projection compiles the flows.
)
from tests.system_e2e.actors import ACTORS

REGISTRY: dict[str, object] = dict(ACTORS)

#: The marches' app module — the projection's compile pass reads the
#: same module object the import above bound (the capability's witness,
#: referenced so the import can never be dropped as unused).
MARCH_APP_MODULE = _wf_app_module

if __name__ == "__main__":
    faulthandler.register(signal.SIGUSR2, all_threads=True)
    settings = WorkerSettings.load()
    try:
        with asyncio.Runner() as runner:
            code = runner.run(_main(settings, actor_registry=REGISTRY))
        with contextlib.suppress(ValueError):  # Why: not the main thread -> no window to guard.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        sys.exit(code)
    except KeyboardInterrupt:
        sys.exit(0)
