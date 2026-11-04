"""THE BOOT PROJECTION'S MEMO: the projection computes once per boot and
shares the result, so its invalidation seams are the whole contract — a
workflow declared after the first read, a second app registered after it,
and the reset seam each must be seen by the next read.
"""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, step
from taskq.workflows import _worker_execution as seam


class Ingest(BaseModel):
    doc_id: str


async def _probe_body(ctx: StepContext, params: Ingest) -> dict[str, str]:
    return {"doc_id": params.doc_id}


def _app_with(workflow_name: str, actor: str) -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow(workflow_name)
    def wf() -> Promise[object]:
        return build(step(_probe_body, Ingest(doc_id="d1"), key="probe", actor=actor))

    return app


def test_a_workflow_declared_after_the_first_projection_is_seen_by_the_next() -> None:
    """The memo's key carries each app's workflow count: a workflow
    declared after the first read changes the count, so the next read
    recomputes and projects it — a worker/test that declares a workflow
    after an earlier boot read is never served the empty projection."""
    seam.reset_app_registry_for_tests()
    try:
        app = WorkflowApp()
        assert seam.project_with_failures() == ([], [])

        @app.workflow("memo-late-workflow")
        def late() -> Promise[object]:
            return build(step(_probe_body, Ingest(doc_id="d1"), key="probe"))

        cohorts, failures = seam.project_with_failures()
        assert [(c.actor, c.queue) for c in cohorts] == [("wf", "default")]
        assert failures == []
    finally:
        seam.reset_app_registry_for_tests()


def test_a_second_app_registered_after_the_first_projection_is_seen() -> None:
    """App registration invalidates the memo: a second app mounted after
    the first read contributes its cohorts to the next projection, and
    the first app's cohort is still there (one merged projection)."""
    seam.reset_app_registry_for_tests()
    try:
        first = _app_with("memo-first-app", actor="memo-one")
        assert [c.actor for c in seam.project_with_failures()[0]] == ["memo-one"]
        second = _app_with("memo-second-app", actor="memo-two")
        assert first is not second
        actors = sorted(c.actor for c in seam.project_with_failures()[0])
        assert actors == ["memo-one", "memo-two"]
    finally:
        seam.reset_app_registry_for_tests()


def test_the_reset_seam_clears_the_memo_with_the_registry() -> None:
    """The reset seam clears the registry AND the memo: after a reset the
    next projection is the honest empty one, never the pre-reset
    cohorts of an app no test owns any more."""
    seam.reset_app_registry_for_tests()
    app = _app_with("memo-reset-app", actor="memo-reset")
    assert [c.actor for c in seam.project_with_failures()[0]] == ["memo-reset"]

    seam.reset_app_registry_for_tests()
    assert seam.iter_imported_apps() == []
    assert seam.project_with_failures() == ([], [])
    assert app is not None


def test_the_compute_paths_lists_are_the_callers_to_mutate() -> None:
    """The projection's callers own their lists on BOTH paths: the
    compute path returns fresh copies too — mutating the first call's
    return never reaches the cache, and the next read (the memo's hit)
    answers from unmutated rows."""
    seam.reset_app_registry_for_tests()
    try:
        app = _app_with("memo-aliasing-app", actor="memo-alias")  # held: the registry is a WeakSet
        cohorts, failures = seam.project_with_failures()  # the compute path
        cohorts.clear()
        failures.append("the caller's mutation must not reach the cache")
        hit_cohorts, hit_failures = seam.project_with_failures()  # the memo's hit
        assert [c.actor for c in hit_cohorts] == ["memo-alias"], (
            "the compute path's return is the caller's to mutate: the "
            "cache's own lists never leave the module on either path"
        )
        assert hit_failures == []
        assert app is not None
    finally:
        seam.reset_app_registry_for_tests()
