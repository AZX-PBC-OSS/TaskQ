"""The walkthrough↔docs binding: every CLI step
``tests/test_ops_flow_walkthroughs.py`` executes must be findable in the docs
it claims to verify.

The walkthroughs are executable truth only while the docs cannot drift away
from them. Reading the docs' fenced ``taskq ...`` invocations (and their
option tables) and comparing against the walkthrough module's OWN source
(the ``_run_cli``/``_arun_cli``/``_spawn_worker`` calls, extracted with
``ast``) closes the loop in both directions:

- a doc that renames a command or drops a flag the walkthrough exercises
  fails here (the executed step is no longer documented);
- a walkthrough that grows a new CLI step must name it in the docs or the
  new step fails here (an undocumented procedure is not a walkthrough).

This is a text contract, deliberately: it pins the doc strings to the
executed strings, not to a paraphrase of them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_WALKTHROUGH = REPO_ROOT / "tests" / "test_ops_flow_walkthroughs.py"
#: The pages the walkthrough module's docstring names as the docs it verifies.
_DOC_PAGES = (
    REPO_ROOT / "docs" / "guides" / "cli.md",
    REPO_ROOT / "docs" / "guides" / "ops.md",
    REPO_ROOT / "docs" / "guides" / "deployment.md",
    REPO_ROOT / "docs" / "guides" / "workgroups.md",
    REPO_ROOT / "docs" / "guides" / "observability.md",
    REPO_ROOT / "docs" / "guides" / "insights.md",
)
#: The runner helpers whose literal argv IS the executed command, plus the
#: ``[*CLI, ...]`` list literal a raw Popen call builds.
_RUNNER_NAMES = {"_run_cli", "_arun_cli", "_spawn_worker"}

#: The chain a Popen list starts with (``[*CLI, "worker", ...]``) - the
#: literal after the star must be the program's first CLI word.
_CLI_STAR = "CLI"


def _walkthrough_cli_requirements() -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    """(command chain, flags) for every CLI invocation the walkthrough runs.

    A chain is the leading run of literal words before the first flag or
    dynamic argument (``"job", "cancel", job_id, "--reason", ...`` gives
    ``("job", "cancel")``); the flags are every ``--word`` literal in the
    call. Values are deliberately excluded: the docs document the command
    and its flags, not the walkthrough's fixture data.
    """
    tree = ast.parse(_WALKTHROUGH.read_text(encoding="utf-8"))
    requirements: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and (node.func.id in _RUNNER_NAMES)
        ):
            literals = [
                arg.value
                for arg in node.args
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
            ]
            if node.func.id == "_spawn_worker" and (not literals or literals[0].startswith("-")):
                # _spawn_worker's own body builds `taskq worker`; its
                # callers pass only extra argv, so the command chain is
                # the helper's, not the call site's.
                literals = ["worker", *literals]
        elif isinstance(node, ast.List) and any(
            isinstance(el, ast.Starred)
            and isinstance(el.value, ast.Name)
            and el.value.id == _CLI_STAR
            for el in node.elts
        ):
            literals = [
                el.value
                for el in node.elts
                if isinstance(el, ast.Constant) and isinstance(el.value, str)
            ]
        else:
            continue
        # The first literal is already the argv[0] slot's content: a runner
        # call spells it directly ("queues", "depth"), a Popen list spells
        # it right after the *CLI star.
        chain: list[str] = []
        rest: list[str] = []
        for i, word in enumerate(literals):
            if word.startswith("-"):
                rest = literals[i:]
                break
            chain.append(word)
        rest = [word for word in rest if word.startswith("--")]
        if chain or rest:
            requirements.append((tuple(chain), tuple(rest)))
    return requirements


def _doc_sections() -> list[tuple[str, str]]:
    """(heading text, section body) for every heading in the docs set.

    A section runs from its heading to the next heading of any level, so
    nested references (``## taskq worker``'s option table, ``### taskq
    actor-config``'s ``set`` example) stay inside the command they belong
    to - which is what makes a flag binding meaningful: the flag must be
    documented ON THE COMMAND the walkthrough executes, not anywhere in
    six pages of prose.
    """
    sections: list[tuple[str, str]] = []
    current_heading = ""
    body: list[str] = []
    for page in _DOC_PAGES:
        for line in page.read_text(encoding="utf-8").splitlines():
            if re.match(r"#{1,6}\s", line):
                if current_heading:
                    sections.append((current_heading, "\n".join(body)))
                current_heading, body = line, []
            elif current_heading:
                body.append(line)
        if current_heading:
            sections.append((current_heading, "\n".join(body)))
            current_heading, body = "", []
    return sections


def _section_for(chain: tuple[str, ...], sections: list[tuple[str, str]]) -> str:
    """The doc section the command chain belongs to (whole docs if no
    heading names it - e.g. a command shown only inside another page's
    playbook table)."""
    if not chain:
        return ""
    head_words = chain[:2]
    exact = re.compile(r"taskq\s+" + r"\s+".join(map(re.escape, head_words)))
    for heading, body in sections:
        if exact.search(heading):
            return body
    # No dedicated section for the exact head (e.g. `actor-config set` is
    # documented under `### taskq actor-config`): the GROUP's own section.
    # Anchored to the taskq prefix so a flag named in a heading
    # (`--force-update-actor-config`) can never shadow the command's.
    group = re.compile(r"taskq\s+" + re.escape(chain[0]) + r"\b")
    for heading, body in sections:
        if group.search(heading):
            return body
    return "\n".join(body for _, body in sections)


def test_every_cli_step_the_walkthroughs_execute_is_documented() -> None:
    """The walkthrough binding, both directions (see the module docstring)."""
    requirements = _walkthrough_cli_requirements()

    # The extractor must see the walkthrough's own steps: a refactor that
    # renames the runners or changes their call shape must re-point this
    # contract, not silently unpin the docs.
    chains = {tuple(chain[:2]) if len(chain) > 1 else chain for chain, _ in requirements}
    for expected in (
        ("queues", "depth"),
        ("doctor",),
        ("actor-config", "set"),
        ("job", "show"),
        ("job", "cancel"),
        ("job", "cancel-where"),
        ("migrate", "status"),
        ("workgroup", "validate"),
        ("health", "live"),
        ("health", "ready"),
        ("health", "metrics"),
        ("worker",),
    ):
        assert expected in chains, (
            f"the walkthrough no longer executes `taskq {' '.join(expected)}` "
            "through a recognised runner - the extractor lost it; re-point the "
            "contract rather than letting steps outside the docs' pin"
        )
    assert requirements, "no CLI steps extracted from the walkthrough module"

    docs_text = "\n".join(page.read_text(encoding="utf-8") for page in _DOC_PAGES)
    sections = _doc_sections()
    undocumted: list[str] = []
    for chain, flags in requirements:
        # The docs document GROUP SUBCOMMAND (two words: "job cancel",
        # "actor-config set") with placeholder values where the walkthrough
        # passes fixture data; a one-word command ("doctor", "worker") is
        # its own chain head.
        head = chain[:2] if len(chain) > 1 else chain
        section = _section_for(chain, sections)
        if head:
            # (?![\w-]) so a renamed command ("queues depth-renamed",
            # "job cancel-where" vs "job cancel") cannot satisfy the pin
            # as a prefix of the drifted name.
            pattern = re.compile(r"taskq\s+" + r"\s+".join(map(re.escape, head)) + r"(?![\w-])")
            if not pattern.search(docs_text):
                undocumted.append(f"`taskq {' '.join(chain)}`: no docs page shows this invocation")
        for flag in flags:
            if flag not in section:
                undocumted.append(
                    f"`taskq {' '.join(chain)} {flag}`: the flag "
                    f"{flag} is not documented on the command's own docs section"
                )
    assert not undocumted, (
        "the walkthroughs execute CLI steps the docs do not document "
        "(a walkthrough step that drifted from, or grew past, its doc):\n"
        + "\n".join(sorted(undocumted))
    )
