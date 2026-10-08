"""TaskQ Workflows — the DAG engine (``taskq[flows]``).

THE IMPORT LAW (§16.1): ``import taskq`` NEVER imports this package. The
only sanctioned entry is a direct ``import taskq.workflows`` (or a
submodule) by a caller that opted into the ``taskq[flows]`` extra. Nothing
under ``src/taskq/`` outside this package may import ``taskq.workflows`` at
module scope — the import-discipline pin convicts a violation.

The engine is core-deps-only (tors, asyncpg, uuid_utils — all base
dependencies); the ``flows`` extra carries no requirements of its own and
exists as the operator-facing opt-in marker.

THE PUBLIC SURFACE IS SMALL ON PURPOSE (§7b's cut): the root re-exports
the AUTHORING verbs + the RUNNER + the USER TYPES — twenty-four names, the
``doc_ingest`` example's imports plus the typed surfaces. Everything else
(SWEEP internals, the finalize mechanics, the SQL statements, the wire
specs, the ledger claims, the progress plumbing) is an ENGINE INTERNAL:
import it from its submodule — the documented path — never from here.
Every root name is a protocol-pin tax paid forever; the internals keep
their homes WITHOUT the tax:

* ``api._graph`` owns the recorder + the wiring verbs; ``api._app`` the
  app/decorators/compiled-workflow/channel; ``api._validate`` owns
  ``validate()``; ``api._mermaid`` owns the emission; ``api._runner`` +
  its concern modules (``_sql_runner``/``_runner_codec``/
  ``_runner_errors``/``_ctx``/``_ctx_wait``/``_runner_loop``/
  ``_runner_chain``/``_runner_ladder``/``_runner_exit`` — §7b's split)
  own create/drive/result; ``api._loop`` owns the loop verbs.
* ``chain`` owns the chain surface; ``context`` owns ``ctx.step``;
  ``definitions`` owns the registered-definition registry;
  ``ledger`` the step-ledger claim + the run-key arbiter; ``engine`` the
  finalize mechanics; ``_sweep`` the sweep arms; ``_types`` the wire
  specs; ``_progress``/``_progress_read``/``_emit`` the streaming +
  progress plumbing; ``_status`` the derivation.
"""

from taskq.workflows.api import (
    CompiledWorkflow,
    Exit,
    Promise,
    WorkflowApp,
    WorkflowBuildError,
    build,
    chain_source,
    gather,
    map_source,
    sink,
    step,
)
from taskq.workflows.api._loop import Done, Refine, loop
from taskq.workflows.api._runner import FlowRunner, StepContext, WorkflowRunError
from taskq.workflows.chain import (
    DONE,
    Chain,
    Route,
    RouterNotTotal,
    Step,
    chain_fork,
    chain_start,
)

__all__ = [
    "DONE",
    "Chain",
    "CompiledWorkflow",
    "Done",
    "Exit",
    "FlowRunner",
    "Promise",
    "Refine",
    "Route",
    "RouterNotTotal",
    "Step",
    "StepContext",
    "WorkflowApp",
    "WorkflowBuildError",
    "WorkflowRunError",
    "build",
    "chain_fork",
    "chain_source",
    "chain_start",
    "gather",
    "loop",
    "map_source",
    "sink",
    "step",
]
