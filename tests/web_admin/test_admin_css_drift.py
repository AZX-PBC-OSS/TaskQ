"""The compiled admin.css cannot go stale (B4).

admin.css is Tailwind's COMPILED output over the templates and the static
JS; the failure mode that shipped was one-directional drift: classes
added to templates while the compiled file stayed as-built (Tailwind
compiles nothing at runtime), so the page rendered dead styles for every
class the compiler had never seen. Two guards hold the property:

1. the in-suite scanner: every Tailwind-shaped token the templates and
   the static JS use must exist in the compiled css - reds locally, no
   Node/npm needed;
2. the CI job (ci.yaml ``admin-css-drift``): recompiles and demands a
   clean tree (``git diff --exit-code``) - the authoritative check, red
   on any scanner hole.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

pytestmark = [pytest.mark.fastapi]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_STATIC = _REPO_ROOT / "src" / "taskq" / "web" / "static"

# Class-shaped tokens that are NOT Tailwind utilities and are therefore
# not expected in the compiled css: the JS/DOM hooks and the hand-written
# rules in _base.html's <style> block.
_CUSTOM_CLASSES: frozenset[str] = frozenset(
    {
        "taskq-badge",
        "job-row",
        "progress-event",
        "progress-bar",
        "progress-bar-wrap",
        "progress-detail",
        "progress-meta",
        "progress-placeholder",
        "sse-console",
        "sse-placeholder",
        "reset-form",
        "deregister-form",
        "internal-htmx-wrapper",
        "animate-slide-down",
        "htmx-indicator",
        "dark",
        "faux Styles",
    }
)


def _used_class_tokens() -> set[str]:
    """Every class token the templates and the static JS ask for."""
    tokens: set[str] = set()
    sources: list[Path] = [
        *(_REPO_ROOT / "src" / "taskq" / "web" / "templates").rglob("*.html"),
        *(_STATIC / "admin.js").glob("*"),
        *(_STATIC / "realtime.js").glob("*"),
    ]
    token_re = re.compile(r"^(?:[a-z0-9._-]+:)*[a-z][a-z0-9._/-]*$")
    for path in sources:
        text = path.read_text()
        attrs = [
            *re.findall(r'class="([^"]*)"', text),
            *re.findall(r"class='([^']*)'", text),
            *re.findall(r'className\s*[:=]\s*"([^"]*)"', text),
        ]
        for attr in attrs:
            # Jinja control flow rides inside class attributes; strip it
            # before splitting so no boundary glue becomes a token.
            clean = re.sub(r"\{%.*?%\}", " ", attr)
            clean = re.sub(r"\{\{.*?\}\}", " ", clean)
            for token in clean.split():
                if token in _CUSTOM_CLASSES or not token_re.match(token):
                    continue
                tokens.add(token)
    return tokens


def _in_css(token: str, css: str) -> bool:
    """The token's escaped form appears in the compiled css.

    Tailwind escapes the special characters a class name carries (the
    variants' colons, the opacity slash, the arbitrary-value brackets) in
    its selectors."""
    escaped = (
        token.replace("\\", "")
        .replace(":", r"\:")
        .replace("/", r"\/")
        .replace(".", r"\.")
        .replace("[", r"\[")
        .replace("]", r"\]")
        .replace("%", r"\%")
    )
    return escaped in css


def test_compiled_css_covers_every_class_the_ui_uses() -> None:
    """B4's teeth: a class added to a template (or the static JS) without
    a rebuild reds here - the stale-compiled-css failure mode, caught in
    a plain pytest run."""
    css = (_STATIC / "admin.css").read_text()
    missing = sorted(t for t in _used_class_tokens() if not _in_css(t, css))
    assert not missing, (
        "classes the templates/JS use are missing from the COMPILED "
        f"admin.css (stale build; run `npm ci && npm run build`): {missing}"
    )


def test_ci_workflow_carries_the_css_drift_gate() -> None:
    """The authoritative half: CI recompiles the css and demands a clean
    tree, so the build cannot go stale even where the scanner above has a
    hole. The job's absence is itself a regression."""
    import yaml

    workflow = yaml.safe_load((_REPO_ROOT / ".github" / "workflows" / "ci.yaml").read_text())
    assert isinstance(workflow, dict)
    jobs = workflow.get("jobs") or {}
    assert "admin-css-drift" in jobs, (
        "ci.yaml lost the admin-css-drift job: the compiled admin.css can "
        "go stale again with nothing to catch it"
    )
    script = " ".join(str(step.get("run", "")) for step in jobs["admin-css-drift"].get("steps", []))
    assert "npm run build" in script, "the drift job must recompile from the source css"
    assert "git diff --exit-code" in script, "the drift job must refuse a dirty compile"


@pytest.mark.parametrize(
    "token",
    [
        "focus:ring-red-500",
        "md:grid-cols-4",
        "p-8",
        "dark:bg-amber-900/40",
        "dark:text-amber-300",
        "dark:bg-blue-900/30",
        "dark:bg-green-900/40",
        "dark:bg-red-900/40",
        "bg-amber-500",
    ],
)
def test_the_stale_builds_missing_classes_are_compiled(token: str) -> None:
    """The nine classes the stale build shipped without (the audit's B4
    finding, each pinned): a rebuild that drops one reds."""
    css = (_STATIC / "admin.css").read_text()
    assert _in_css(token, css), f"{token} is missing from the compiled admin.css"
