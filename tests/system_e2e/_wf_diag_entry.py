"""The diagnostic workflow-capable worker entry for the capacity-cohort
control.

Same fleet standard as :mod:`tests.system_e2e._wf_entry` (the app-module
import is the capability; the same registry; the same post-drain guard,
carried by ``worker_main_async(..., ignore_sigterm_on_exit=True)``),
but it enters through the production :func:`worker_main_async` wrapper —
whose ``setup_logging`` applies the settings' ``TASKQ_LOG_LEVEL`` — so the
boot's INFO-level capacity diagnostics (the operator-owned
``actor-config-capacity-override`` mismatch event among them) surface in
the worker's own log sink. The shared ``_wf_entry`` enters the bare
``_main`` bootstrap on purpose (the pre-setup default chain filters INFO
at stdlib's root WARNING — the quiet fleet default), which is exactly why
the mismatch control's workers need this entry.
"""

import asyncio
import faulthandler
import signal
import sys

from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main_async
from tests.system_e2e import (
    _wf_app as _wf_app_module,  # Why: the import is the capability — the app registry observes it, the boot's projection compiles the flows.
)
from tests.system_e2e.actors import ACTORS

REGISTRY: dict[str, object] = dict(ACTORS)
REGISTRY.update(_wf_app_module.MARCH_ACTORS)

#: The marches' app module — the projection's compile pass reads the
#: same module object the import above bound (the capability's witness,
#: referenced so the import can never be dropped as unused).
MARCH_APP_MODULE = _wf_app_module

if __name__ == "__main__":
    faulthandler.register(signal.SIGUSR2, all_threads=True)
    settings = WorkerSettings.load()
    try:
        with asyncio.Runner() as runner:
            # cron_registry=[] (not None): no auto-discovery — this control's
            # schema must never have the march apps' cron flows fired into it;
            # only the fleet entry's exact bootstrap surface, plus the
            # production logging setup.
            #
            # ignore_sigterm_on_exit=True: this loop is the caller's, so
            # the guard runs where worker_main_async hands back — SIGTERM
            # masked, this loop's handler removed, ignored, mask restored
            # — and the Runner's close below finds no handler to reset.
            code = runner.run(
                worker_main_async(
                    settings,
                    actor_registry=REGISTRY,
                    cron_registry=[],
                    ignore_sigterm_on_exit=True,
                )
            )
        sys.exit(code)
    except KeyboardInterrupt:
        sys.exit(0)
