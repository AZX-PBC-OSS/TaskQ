"""The flow API (T09) — the authoring surface over the DAG engine.

The public surface, behind ``taskq[flows]`` (the import law: ``import
taskq`` NEVER imports this package). The wiring:

    from taskq.workflows import WorkflowApp, gather, step

    app = WorkflowApp()

    @app.actor(queue="cpu")
    async def fetch(ctx, params: Ingest) -> Report: ...

    @app.workflow("doc_ingest")
    async def doc_ingest() -> object:
        fetched = step(fetch, Ingest(doc_id="d1"))   # Promise[Report]
        both = gather([fetched])                      # the ALL-upstream join
        return build(consume(both))                   # the completeness point

Module homes: ``_graph`` owns the recorder + the wiring verbs; ``_app``
owns the app/decorators/compiled-workflow/channel; ``_validate`` owns
``validate()``; ``_mermaid`` owns the emission; ``_runner`` owns
create/drive/result.
"""

from taskq.workflows.api._app import (
    CompiledWorkflow,
    SignalChannel,
    TypedGate,
    WorkflowActor,
    WorkflowApp,
)
from taskq.workflows.api._graph import (
    GateDecl,
    NodeDecl,
    Promise,
    WorkflowBuildError,
    build,
    gather,
    map_source,
    sink,
    step,
)

__all__ = [
    "CompiledWorkflow",
    "GateDecl",
    "NodeDecl",
    "Promise",
    "SignalChannel",
    "TypedGate",
    "WorkflowActor",
    "WorkflowApp",
    "WorkflowBuildError",
    "build",
    "gather",
    "map_source",
    "sink",
    "step",
]
