"""THE BOOT GATE'S DIAGNOSIS: a refusal must name the condition that is
actually true. The gate now refuses a mounted app with no runnable
workflow — but an imported, empty app and a module that never imported an
app are two different defects with two different remedies, and the
refusal is the operator's only clue at boot.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import typer

from taskq.cli import _load_actor_registry


def _write_module(directory: Path, name: str, body: str) -> str:
    sys.modules.pop(name, None)
    (directory / f"{name}.py").write_text(body)
    return name


def test_an_empty_mounted_app_refusal_does_not_claim_no_app_is_imported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A module that imports and mounts ``WorkflowApp()`` but registers no
    workflow is refused — correctly — because it projects no runnable
    cohort. The refusal must name THAT condition (an imported app that
    declares no workflow). Saying "no WorkflowApp is imported in the
    module" is false for this module: the operator is told to add the very
    import that is already there, and the real defect (the app declares
    nothing) goes unstated.
    """
    monkeypatch.syspath_prepend(str(tmp_path))
    module = _write_module(
        tmp_path,
        "wf_boot_empty_app_diagnosis",
        "from taskq.workflows import WorkflowApp\n\napp = WorkflowApp()\n\nACTORS = {}\n",
    )

    with pytest.raises(typer.Exit):
        _load_actor_registry(f"{module}:ACTORS")

    err = capsys.readouterr().err
    assert "no WorkflowApp is imported" not in err, (
        "the empty mounted app was refused with 'no WorkflowApp is imported "
        "in the module' — false: the module imports and mounts one; the "
        "operator is sent to add an import that already exists instead of "
        "being told the app declares no workflow"
    )


def test_an_empty_graph_refusal_does_not_claim_no_app_is_imported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A workflow builder that wires nothing and returns ``None`` compiles
    (zero nodes, no terminal) and projects no cohort; the app IS imported.
    The refusal for that module must not claim no app was imported — the
    empty graph is the defect, and it is the exact forgetting-a-return
    accident a builder can hit.
    """
    monkeypatch.syspath_prepend(str(tmp_path))
    module = _write_module(
        tmp_path,
        "wf_boot_empty_graph_diagnosis",
        """from taskq.workflows import Promise, WorkflowApp

app = WorkflowApp()


@app.workflow("forgot-the-wiring")
def forgot_the_wiring() -> Promise[object]:
    return None  # the forgotten builder return


ACTORS = {}
""",
    )

    with pytest.raises(typer.Exit):
        _load_actor_registry(f"{module}:ACTORS")

    err = capsys.readouterr().err
    assert "no WorkflowApp is imported" not in err, (
        "the empty-graph workflow was refused with 'no WorkflowApp is "
        "imported in the module' — false: the app is imported and its "
        "workflow compiled to zero nodes; the operator is not told the "
        "builder wired nothing"
    )


def test_the_refusal_names_every_failed_workflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The projection reports one failure per skipped workflow; the
    refusal renders ALL of them. An app with two broken declarations must
    name both — naming only the first leaves the second to the next boot
    after the operator fixes the first."""
    monkeypatch.syspath_prepend(str(tmp_path))
    module = _write_module(
        tmp_path,
        "wf_boot_two_failures_diagnosis",
        """from taskq.workflows import Promise, WorkflowApp

app = WorkflowApp()


@app.workflow("broken-first")
def broken_first() -> Promise[object]:
    raise RuntimeError("the first declaration is broken")


@app.workflow("broken-second")
def broken_second() -> Promise[object]:
    raise RuntimeError("the second declaration is broken")


ACTORS = {}
""",
    )

    with pytest.raises(typer.Exit):
        _load_actor_registry(f"{module}:ACTORS")

    err = capsys.readouterr().err
    assert "broken-first" in err
    assert "broken-second" in err
