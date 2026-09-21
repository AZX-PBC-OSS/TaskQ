"""Error-path pins for the CLI's ``module:attr`` reference resolution.

``--actors`` and the credential-provider flags resolve a ``module:attr``
ref at startup. The refusal paths are operator-facing contracts: each
must print a message naming the ref and the shape problem and exit 1,
never a traceback and never a silent fallback to defaults (a credential
path that silently fell back to the DSN would authenticate with a static
password and look healthy until the first reconnect after the deploy).

The loaders are called directly (the established tier,
tests/test_silent_failure_guards.py); the message goes to stderr via
``typer.echo`` and the refusal is the raised ``typer.Exit(code=1)``.
"""

import sys
from pathlib import Path

import pytest
import typer

from taskq.cli import _load_actor_registry, _resolve_provider


@pytest.fixture
def module_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A scratch directory on sys.path for ref-target modules."""
    monkeypatch.syspath_prepend(str(tmp_path))
    return tmp_path


def _write_module(directory: Path, name: str, body: str) -> str:
    """Write an importable module and return its ``module:attr``-ready name.

    A failed import must not leave a poisoned entry in sys.modules to
    mask a later test's refusal, so each write starts from a clean name.
    """
    sys.modules.pop(name, None)
    (directory / f"{name}.py").write_text(body)
    return name


def _ref(module: str, attr: str) -> str:
    return f"{module}:{attr}"


# ── _import_ref's refusals ───────────────────────────────────────────────


def test_a_module_that_raises_at_import_names_the_cause_and_exits_1(
    module_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The generic-import-failure arm: ModuleNotFoundError gets its own
    message ("module not found"); every other import-time exception must
    name the cause, or an operator debugs a config bug from a bare exit
    code."""
    module = _write_module(
        module_dir, "cli_ref_boom_module", "raise RuntimeError('boom at import')\n"
    )

    with pytest.raises(typer.Exit):
        _load_actor_registry(_ref(module, "ACTORS"))

    err = capsys.readouterr().err
    assert "failed to import module" in err
    assert "boom at import" in err


def test_an_iterable_of_non_actor_refs_is_refused(
    module_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``[1, 2, 3]`` is iterable, so the Mapping arm skips it and the
    element-type guard must catch the non-ActorRef items: an unvetted
    registry would dispatch jobs to callables the ``@actor`` decorator
    never validated."""
    module = _write_module(module_dir, "cli_ref_nonref_module", "ACTORS = [1, 2, 3]\n")

    with pytest.raises(typer.Exit):
        _load_actor_registry(_ref(module, "ACTORS"))

    assert "expected Mapping[str, ActorRef] or Iterable[ActorRef]" in capsys.readouterr().err


def test_an_empty_registry_refuses_to_boot(
    module_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """An empty mapping passes every type guard and would boot a worker
    that dispatches nothing — the one silent failure mode worse than a
    crash. The loader must refuse it."""
    module = _write_module(module_dir, "cli_ref_empty_module", "ACTORS = {}\n")

    with pytest.raises(typer.Exit):
        _load_actor_registry(_ref(module, "ACTORS"))

    err = capsys.readouterr().err
    assert "is empty" in err
    assert "refusing to boot" in err


# ── the provider shapes ─────────────────────────────────────────────────


def test_a_provider_factory_that_raises_surfaces_the_exception_and_exits_1(
    module_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A zero-arg factory the CLI calls at startup must surface the call's
    exception and exit 1 — never fall back to the DSN and look healthy
    until the first reconnect after the deploy."""
    module = _write_module(
        module_dir,
        "cli_ref_bad_factory",
        "def make_provider():\n    raise RuntimeError('vault is down')\n",
    )

    with pytest.raises(typer.Exit):
        _resolve_provider(
            _ref(module, "make_provider"),
            option="--pg-credential-provider",
            method="get_pg_credential",
        )

    err = capsys.readouterr().err
    assert "calling" in err and "raised RuntimeError: vault is down" in err


def test_a_provider_object_without_the_method_is_refused(
    module_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """An instance without the async ``get_pg_credential()`` method (the
    wrong provider wired to the wrong flag) must be refused at startup
    with the shape guidance, not at the first credential rotation."""
    module = _write_module(
        module_dir,
        "cli_ref_wrong_shape",
        "class NotAProvider:\n    pass\n\nPROVIDER = NotAProvider()\n",
    )

    with pytest.raises(typer.Exit):
        _resolve_provider(
            _ref(module, "PROVIDER"),
            option="--pg-credential-provider",
            method="get_pg_credential",
        )

    err = capsys.readouterr().err
    assert "does not implement async get_pg_credential()" in err
