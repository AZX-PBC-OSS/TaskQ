"""NEGATIVE TYPE PROBES — the workflow DECLARATION door (the typed
decorator's outermost face).

The typed-doors law's FIRST line: the type story cannot die at the first
line the user writes. ``@app.workflow``'s parameter is
``Callable[[], Promise[R]]`` — :class:`Promise` is COVARIANT, so every
``Promise[X]`` satisfies the door (R carried intact through the generic),
and the OLD declaration face — a build function declared ``-> object``,
left unannotated, or ``async`` — is a STATIC ERROR at the decoration
site, on both pinned checkers. The probes:

* ``probe_object_decl`` — the fleet's pre-door shape, ``def f() ->
  object`` while actually returning ``build(...)``'s ``Promise[R]``: the
  erasure is refused AT THE DECORATION (pyright ``reportArgumentType``,
  ty ``invalid-argument-type`` — the decorator line).
* ``probe_object_decl_direct`` — the same lie through the direct-call
  form (the decorator returned, then applied): the parameter's type is
  checked at the argument, not just at the ``@`` application.
* ``probe_async_decl`` — an ``async def`` builder: the build function is
  SYNC and PURE (the recorder's verbs never await); a coroutine return
  is not a ``Promise`` — the same refusal. (The UNANNOTATED builder is
  the one face the checkers defer on — an implicitly-typed function
  skips the decorator's parameter check — so the corpus cannot pin it;
  the DECLARED erasure is the door's own refusal, and the compile's
  ``TypeError`` keeps the unannotated None return's runtime face.)

The GREEN face (clean lines, the two-faces boundary): a ``-> Promise[Report]``
decl and a ``-> Promise[object]`` decl both pass UNMARKED — covariance is
the acceptance, not an ``Any`` escape hatch.
"""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows import Promise, WorkflowApp, build, step


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


async def _body(ctx: object, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


def probe_object_decl_refused() -> None:
    """THE CONVICTION: the fleet's ``-> object`` decl declares the
    erasure while the body returns ``build(...)``'s ``Promise[R]`` — the
    decorator refuses it at the decoration site (the marker trails the
    decorator line, the line BOTH checkers name)."""
    app = WorkflowApp()

    @app.workflow(
        "probe_object_decl"
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the -> object decl refused at the decoration
    def probe_object_decl() -> object:
        wired = step(_body, Ingest(doc_id="d"), key="fetch")
        return build(wired)

    _ = app


def probe_object_decl_direct_refused() -> None:
    """The direct-call form: the decorator returned, then applied — the
    parameter's type is checked at the ARGUMENT (the same refusal, the
    call's own line)."""
    app = WorkflowApp()

    def bad_builder() -> object:
        wired = step(_body, Ingest(doc_id="d"), key="fetch")
        return build(wired)

    app.workflow("probe_direct_decl")(
        bad_builder
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the -> object decl refused at the argument
    _ = app


def probe_async_decl_refused() -> None:
    """The build function is SYNC and PURE — an ``async def`` builder
    returns a coroutine, never a ``Promise``: refused at the decoration
    (the old annotation accepted it in silence)."""
    app = WorkflowApp()

    @app.workflow(
        "probe_async_decl"
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the async decl's coroutine is not a promise
    async def probe_async_decl() -> object:
        wired = step(_body, Ingest(doc_id="d"), key="fetch")
        return build(wired)

    _ = app


def probe_typed_decls_green() -> None:
    """THE GREEN FACE (clean lines — no marker, no error): the concrete
    ``Promise[Report]`` decl and the covariant ``Promise[object]`` decl
    both satisfy the door; R rides the generic into the registry."""
    app = WorkflowApp()

    @app.workflow("probe_green_concrete")
    def probe_green_concrete() -> Promise[Report]:
        wired = step(_body, Ingest(doc_id="d"), key="fetch")
        return build(wired)

    @app.workflow("probe_green_covariant")
    def probe_green_covariant() -> Promise[object]:
        wired = step(_body, Ingest(doc_id="d"), key="fetch")
        return build(wired)

    _ = app
