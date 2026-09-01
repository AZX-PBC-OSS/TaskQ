"""Guards on how CI and the Makefile invoke uv.

The defect class these exist to prevent is invisible to every other check in the repo: the YAML
is valid, the commands succeed, and the suite goes green -- against a different set of packages
than the one the job installed.

A bare ``uv run`` re-resolves the interpreter and re-syncs on every invocation. That implicit
sync is inexact, so it will not uninstall an extra that is already present; but when it decides
the virtual environment it found is on the wrong interpreter, it REMOVES that environment and
rebuilds it from the DEFAULT dependency set. Every extra and every non-default group installed by
the preceding ``uv sync`` is gone. Measured on this project: 116 packages before, 111 after, and
``import fastapi`` failing in a leg that had asked for ``--extra fastapi``.

``--locked`` is the other half. Both ``--locked`` and ``--frozen`` refuse to re-resolve, so
neither can install a version ``uv.lock`` never described. They differ on what happens when
``pyproject.toml`` and ``uv.lock`` disagree: ``--frozen`` ignores the disagreement and produces a
green job silently missing a newly added dependency, while ``--locked`` asserts the lock is up to
date and fails. ``--frozen`` is therefore banned outright here, not merely discouraged.

The structural half of the fix is ``.github/actions/setup-uv-env``: one composite action owns
interpreter pinning and the single sync, so compliance is a property of the workflow's shape
rather than of every step carrying the right flags. The tests below check both -- that jobs go
through the action, and that nothing downstream re-syncs anyway.

Every exception is a named entry carrying its own justification, so the allowlist cannot widen
without someone writing down why.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import Any, cast

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
_WORKFLOW_DIR = _REPO_ROOT / ".github" / "workflows"
_ACTION_DIR = _REPO_ROOT / ".github" / "actions"
_SETUP_ACTION = "./.github/actions/setup-uv-env"

_WORKFLOWS = sorted(_WORKFLOW_DIR.glob("*.y*ml"))
_ACTIONS = sorted(_ACTION_DIR.glob("*/action.y*ml"))

# uv flags that consume the following token as their value. Needed to tell a flag's value apart
# from the command being run: in `uv run --python 3.12 pytest`, the flag list ends at `pytest`,
# not at `3.12`. Getting this wrong in the permissive direction lets a real bare `uv run`
# through; getting it wrong in the strict direction produces a false failure on a compliant
# command, which is how a guard like this loses its audience.
_VALUE_FLAGS = frozenset(
    {
        "--python",
        "--with",
        "--with-editable",
        "--with-requirements",
        "--group",
        "--extra",
        "--only-group",
        "--no-group",
        "--index",
        "--index-url",
        "--extra-index-url",
        "--directory",
        "--project",
        "--package",
        "--refresh-package",
        "--python-preference",
        "--color",
        "--cache-dir",
        "--config-file",
        "--config-setting",
    }
)


# --- workflow structure -----------------------------------------------------------------------
#
# yaml.safe_load returns Any. It is cast once, here, to a declared type rather than allowed to
# spread untyped through the module, so pyright strict has something to check the accessors
# against instead of silently accepting anything.

_YamlMap = dict[str, Any]


def _load_yaml(path: Path) -> _YamlMap:
    document = cast(object, yaml.safe_load(path.read_text(encoding="utf-8")))
    assert isinstance(document, dict), f"{path.name}: top level is not a mapping"
    return cast(_YamlMap, document)


def _jobs(path: Path) -> dict[str, _YamlMap]:
    jobs = cast(object, _load_yaml(path).get("jobs") or {})
    assert isinstance(jobs, dict), f"{path.name}: `jobs` is not a mapping"
    return cast("dict[str, _YamlMap]", jobs)


def _steps(job: _YamlMap) -> list[_YamlMap]:
    steps = cast(object, job.get("steps") or [])
    assert isinstance(steps, list), "`steps` is not a list"
    return [step for step in cast("list[object]", steps) if isinstance(step, dict)]


def _run_scripts(job: _YamlMap) -> str:
    return " ".join(str(step.get("run", "")) for step in _steps(job))


def _uv_invocations(text: str, subcommand: str) -> list[tuple[int, tuple[str, ...]]]:
    """(line number, flags) for every ``uv <subcommand>`` in *text*, comments stripped.

    Flags are the ``--`` tokens between the subcommand and the first positional argument, with
    the values of value-taking flags skipped. Returning the flags rather than the raw string is
    what makes ``uv run --python 3.12 --no-sync pytest`` compliant: a regex that stops at the
    first flag whose value is a separate token never sees the ``--no-sync`` that follows it.

    *text* must be shell, never raw YAML. Every one of these files documents its own uv rules in
    prose, and a scanner pointed at the raw file reports the explanation as a violation -- a `#`
    strip does not help, because YAML prose is a block scalar, not a comment. `_sources`
    is what extracts the shell.
    """
    found: list[tuple[int, tuple[str, ...]]] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        code = raw.split("#", 1)[0]
        for match in re.finditer(rf"\buv\s+{subcommand}\b", code):
            rest = code[match.end() :]
            try:
                tokens = shlex.split(rest, comments=False)
            except ValueError:
                # Unbalanced quotes: the tail of a shell line we only half-see. Fall back to a
                # whitespace split rather than skipping the line, which would drop a real hit.
                tokens = rest.split()
            flags: list[str] = []
            index = 0
            while index < len(tokens):
                token = tokens[index]
                if not token.startswith("-"):
                    break
                flags.append(token.split("=", 1)[0])
                index += 2 if token in _VALUE_FLAGS else 1
            found.append((number, tuple(flags)))
    return found


def _makefile_text() -> str:
    """The Makefile with its uv-related variables expanded.

    ``$(UVRUN)`` and ``$(SYNC_ARGS)`` are the whole point of the Makefile's discipline, and a
    scanner that reads the file literally sees one guarded ``uv run`` at the definition and
    nothing at the call sites -- so every call site would go unchecked. Expanding the variables
    first means the recipes are audited as they actually run.
    """
    text = (_REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    definitions: dict[str, str] = dict(
        re.findall(r"^(\w+)\s*:?=\s*(.+)$", text, flags=re.MULTILINE)
    )
    assert "UVRUN" in definitions, "Makefile no longer defines UVRUN; update this scanner"
    for name, value in definitions.items():
        text = text.replace(f"$({name})", value)
    return text


def _sources() -> list[tuple[str, str]]:
    """(location label, shell code) for every place in the repo that can invoke uv.

    Only real shell: a workflow's and a composite action's ``run:`` scripts, plus the expanded
    Makefile. Nothing else in a YAML file can execute a command, and everything else in these
    particular files is prose explaining the very rules being checked.
    """
    sources: list[tuple[str, str]] = []
    for path in _WORKFLOWS:
        for job_name, job in _jobs(path).items():
            for step in _steps(job):
                script = str(step.get("run", ""))
                if script:
                    sources.append((f"{path.name}:{job_name}", script))
    for path in _ACTIONS:
        label = f"{path.parent.name}/{path.name}"
        runs = cast(_YamlMap, _load_yaml(path).get("runs") or {})
        steps = cast(object, runs.get("steps") or [])
        assert isinstance(steps, list), f"{label}: `runs.steps` is not a list"
        for step in cast("list[object]", steps):
            if isinstance(step, dict):
                script = str(cast(_YamlMap, step).get("run", ""))
                if script:
                    sources.append((label, script))
    sources.append(("Makefile", _makefile_text()))
    return sources


# --- exceptions -------------------------------------------------------------------------------
#
# Keyed by the exact flag tuple, so adding a flag to a command drops it out of the allowlist and
# back under the rule. Each entry states why the command is safe, not merely that it is allowed.

_UV_RUN_EXCEPTIONS: dict[tuple[str, ...], str] = {
    ("--no-project",): (
        "ci.yaml's `build` job inspects the built sdist/wheel with stdlib-only commands "
        "(`python -m tarfile`, `python -m zipfile`). That job deliberately syncs nothing, so "
        "there is no environment to preserve: `--no-sync` would have uv create an EMPTY .venv "
        "(it stops uv populating a missing environment, not creating one) and a bare `uv run` "
        "would build and populate a whole .venv just to reach the stdlib. `--no-project` uses no "
        "project environment at all, so nothing can be pruned or re-resolved."
    ),
}

_UV_SYNC_EXCEPTIONS: dict[tuple[str, ...], str] = {}


# --- scanner self-guards ----------------------------------------------------------------------


def test_the_scanner_finds_real_invocations() -> None:
    """Guards the guards. Every assertion below is satisfied by an empty scan, so a scanner that
    silently returned nothing would leave them all passing while checking nothing."""
    assert _WORKFLOWS, f"no workflows found under {_WORKFLOW_DIR}; has the directory moved?"
    assert _ACTIONS, f"no composite actions found under {_ACTION_DIR}"
    runs = [(name, hit) for name, text in _sources() for hit in _uv_invocations(text, "run")]
    syncs = [(name, hit) for name, text in _sources() for hit in _uv_invocations(text, "sync")]
    assert runs, "no `uv run` invocations found; the scanner is broken"
    assert syncs, "no `uv sync` invocations found; the scanner is broken"
    assert any(name == "Makefile" for name, _ in runs), (
        "no `uv run` seen in the Makefile; $(UVRUN) expansion has regressed and every call "
        "site is going unchecked"
    )
    assert any(name.startswith("ci.yaml:") for name, _ in runs), "no `uv run` seen in ci.yaml"
    assert any(name.startswith("setup-uv-env/") for name, _ in syncs), (
        "no `uv sync` seen in the shared setup action; the action scan has regressed"
    )


def test_the_scanner_reads_flags_past_a_flag_that_takes_a_value() -> None:
    r"""Pins the exact parsing bug this scanner is written to avoid.

    Collecting flags with a naive ``(--\S+\s+)*`` regex stops at the first flag whose value is a
    separate token, so ``uv run --python 3.12 --no-sync pytest`` reports its flags as
    ``('--python',)`` and is condemned as a bare ``uv run``. A guard that fails on compliant
    commands gets entries added to its allowlist until it means nothing.
    """
    assert dict(_uv_invocations("uv run --python 3.12 --no-sync pytest -n 4", "run"))[1] == (
        "--python",
        "--no-sync",
    )
    assert dict(_uv_invocations("uv run --with pip-audit --no-sync pip-audit", "run"))[1] == (
        "--with",
        "--no-sync",
    )
    # `--flag=value` form, and a bare invocation, must both parse.
    assert dict(_uv_invocations("uv run --python=3.12 pytest", "run"))[1] == ("--python",)
    assert dict(_uv_invocations("uv run pytest", "run"))[1] == ()
    # A commented-out command is prose, not an invocation.
    assert _uv_invocations("# uv run pytest", "run") == []


# --- the rules --------------------------------------------------------------------------------


def test_every_uv_run_is_no_sync() -> None:
    """A bare ``uv run`` can discard the environment the preceding ``uv sync`` built.

    Every ``uv run`` in this repo sits downstream of an explicit sync that asked for extras and
    non-default groups, so an implicit sync can only ever narrow what that command was given --
    and when the interpreter disagrees it does not narrow the environment, it deletes it.
    """
    offenders: list[str] = []
    for name, text in _sources():
        for number, flags in _uv_invocations(text, "run"):
            if "--no-sync" in flags or flags in _UV_RUN_EXCEPTIONS:
                continue
            offenders.append(f"{name}:{number} (flags: {list(flags)})")
    assert not offenders, (
        "`uv run` without --no-sync re-resolves the interpreter and re-syncs, and can rebuild "
        "the environment from the default dependency set, dropping every extra and non-default "
        f"group: {offenders}. Add --no-sync, or add a justified entry to _UV_RUN_EXCEPTIONS."
    )


def test_every_uv_sync_is_locked() -> None:
    """``--locked`` fails the job when ``uv.lock`` no longer matches ``pyproject.toml``, rather
    than quietly re-resolving and installing a dependency set nobody reviewed."""
    offenders: list[str] = []
    for name, text in _sources():
        for number, flags in _uv_invocations(text, "sync"):
            if "--locked" in flags or "--check" in flags or flags in _UV_SYNC_EXCEPTIONS:
                continue
            offenders.append(f"{name}:{number} (flags: {list(flags)})")
    assert not offenders, (
        "`uv sync` without --locked may silently re-resolve and install a set that does not "
        f"match the committed lock: {offenders}"
    )


def test_frozen_is_never_used() -> None:
    """``--frozen`` and ``--locked`` both refuse to re-resolve, so the choice between them is
    only about what happens when ``pyproject.toml`` and ``uv.lock`` disagree. ``--frozen`` treats
    the lock as the source of truth and ignores the disagreement, so a dependency added without
    re-locking yields a green job silently missing it. ``--locked`` fails instead. Nothing in
    this repo wants the weaker one: both files are always present together, so the comparison is
    local and costs nothing.
    """
    offenders = [
        f"{name}:{number}"
        for name, text in _sources()
        for subcommand in ("run", "sync")
        for number, flags in _uv_invocations(text, subcommand)
        if "--frozen" in flags
    ]
    assert not offenders, (
        "use --locked, not --frozen: --frozen ignores a pyproject.toml/uv.lock disagreement and "
        f"ships an environment silently missing a newly added dependency: {offenders}"
    )


def test_uv_run_never_injects_an_undeclared_dependency() -> None:
    """``uv run --with X`` resolves X outside the lock, so the command runs against a package no
    reviewer approved and no other lane shares.

    This repo had ``uv run --with pip-audit pip-audit`` in the security job while ``pip-audit``
    was already declared in the ``dev`` group: the flag injected a second, separately resolved
    copy that shadowed the locked one, so the tool auditing the locked dependency set was itself
    not part of it. Anything needed at runtime belongs in a dependency group.
    """
    injecting = {"--with", "--with-requirements", "--with-editable"}
    offenders = [
        f"{name}:{number}"
        for name, text in _sources()
        for number, flags in _uv_invocations(text, "run")
        if injecting.intersection(flags)
    ]
    assert not offenders, (
        "`uv run --with` installs a dependency that uv.lock does not describe; declare it in a "
        f"[dependency-groups] entry instead: {offenders}"
    )


def test_every_job_that_uses_uv_goes_through_the_shared_setup_action() -> None:
    """The structural half of the fix, and the reason the rules above are hard to break.

    When each job installs uv, selects an interpreter and syncs for itself, compliance depends on
    every one of those steps carrying the right flags forever, and a new job copied from an old
    one inherits whatever the old one got wrong. Routing every job through one composite action
    means a job declares only which extras and groups it needs; the interpreter pin and the
    single ``--locked`` sync are not its business and cannot be omitted from it.
    """
    offenders: list[str] = []
    for path in _WORKFLOWS:
        for job_name, job in _jobs(path).items():
            scripts = _run_scripts(job)
            if "uv sync" not in scripts and "uv run" not in scripts:
                continue
            if any(step.get("uses") == _SETUP_ACTION for step in _steps(job)):
                continue
            offenders.append(f"{path.name}:{job_name}")
    assert not offenders, (
        f"these jobs invoke uv without going through `{_SETUP_ACTION}`, so their interpreter "
        f"pin and their sync flags are theirs to get right by hand: {offenders}"
    )


def test_no_job_installs_uv_or_selects_an_interpreter_behind_the_action_s_back() -> None:
    """A job that calls ``setup-uv`` or ``uv python install`` itself has reintroduced the
    per-job setup the shared action exists to remove, and its interpreter can then disagree with
    the one the action pinned."""
    offenders: list[str] = []
    for path in _WORKFLOWS:
        for job_name, job in _jobs(path).items():
            for step in _steps(job):
                if "astral-sh/setup-uv" in str(step.get("uses", "")):
                    offenders.append(f"{path.name}:{job_name} (installs uv directly)")
            if "uv python install" in _run_scripts(job):
                offenders.append(f"{path.name}:{job_name} (selects an interpreter directly)")
    assert not offenders, (
        f"uv setup belongs in `{_SETUP_ACTION}`, which every job already uses: {offenders}"
    )


def test_the_setup_action_pins_the_interpreter_for_the_whole_job() -> None:
    """``UV_PYTHON`` via ``$GITHUB_ENV``, not a per-command ``--python``.

    ``uv python install 3.12`` only downloads an interpreter; it does not select one. A later
    ``uv sync`` with no ``--python`` reads ``.python-version`` (3.13 here) and builds a 3.13
    environment under a step labelled "Set up Python 3.12" -- several jobs used to advertise one
    interpreter and use another.

    A per-command ``--python`` is not enough either: it decides one command's interpreter and
    leaves every other uv call in the job free to disagree, and a ``uv run`` that disagrees
    deletes the environment and rebuilds it from the default dependency set. ``UV_PYTHON`` is
    read by ``sync`` and ``run`` alike, and writing it to ``$GITHUB_ENV`` makes it outlive the
    composite action's own shells.
    """
    action = _ACTION_DIR / "setup-uv-env" / "action.yml"
    assert action.is_file(), f"{action} is missing; every workflow depends on it"
    text = action.read_text(encoding="utf-8")
    assert "UV_PYTHON=" in text and "GITHUB_ENV" in text, (
        "the shared action no longer exports UV_PYTHON to $GITHUB_ENV, so its interpreter pin "
        "dies with the action's own steps and the calling job's uv calls are free to re-resolve"
    )
    assert "uv sync --locked" in text, "the shared action's sync is no longer --locked"


def test_a_python_matrix_job_passes_the_matrix_value_to_the_setup_action() -> None:
    """The matrix legs are where a wrong interpreter is worst: every leg still passes, but they
    all ran the same version, so the matrix silently tested one interpreter instead of three.
    """
    offenders: list[str] = []
    for path in _WORKFLOWS:
        for job_name, job in _jobs(path).items():
            strategy = cast(_YamlMap, job.get("strategy") or {})
            matrix = cast(_YamlMap, strategy.get("matrix") or {})
            if not matrix.get("python-version"):
                continue
            passed = [
                cast(_YamlMap, step.get("with") or {}).get("python-version")
                for step in _steps(job)
                if step.get("uses") == _SETUP_ACTION
            ]
            if passed != ["${{ matrix.python-version }}"]:
                offenders.append(f"{path.name}:{job_name} (got {passed!r})")
    assert not offenders, (
        "a job with a `python-version` matrix must pass "
        f"`python-version: ${{{{ matrix.python-version }}}}` to `{_SETUP_ACTION}`, or every leg "
        f"runs the same default interpreter: {offenders}"
    )


def test_the_matrix_job_asserts_it_is_running_the_interpreter_it_advertises() -> None:
    """Belt and braces for the leg that cannot show this failure any other way.

    An interpreter mismatch stays silent for as long as the imports happen to resolve, which is
    the property that lets it survive review. An explicit version assertion in the job turns it
    into an immediate, named failure.
    """
    ci = next(wf for wf in _WORKFLOWS if wf.name == "ci.yaml")
    checked = False
    for job_name, job in _jobs(ci).items():
        strategy = cast(_YamlMap, job.get("strategy") or {})
        matrix = cast(_YamlMap, strategy.get("matrix") or {})
        if not matrix.get("python-version"):
            continue
        checked = True
        assert "sys.version_info" in _run_scripts(job), (
            f"ci.yaml:{job_name} fans out over interpreters but never checks which one it got"
        )
    assert checked, "ci.yaml no longer has a python-version matrix; this guard is now vacuous"


def test_every_documented_exception_is_still_in_use() -> None:
    """An allowlist entry whose command has been fixed or deleted is a hole waiting for the next
    command that happens to share its flags. Removing dead entries keeps the list honest."""
    live_run = {flags for _, text in _sources() for _, flags in _uv_invocations(text, "run")}
    live_sync = {flags for _, text in _sources() for _, flags in _uv_invocations(text, "sync")}
    stale = [str(key) for key in _UV_RUN_EXCEPTIONS if key not in live_run]
    stale += [str(key) for key in _UV_SYNC_EXCEPTIONS if key not in live_sync]
    assert not stale, f"unused entries in the uv allowlist; delete them: {stale}"
