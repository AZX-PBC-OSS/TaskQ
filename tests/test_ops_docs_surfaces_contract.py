"""Docs-contract pin: every admin surface the ops docs point an operator at
must exist in the shipped admin router.

ops.md §12 (the scaling playbook) routes operators to admin pages -
``/admin/actors`` to see an actor pinned at its cap, ``/admin/queues/{queue}``
for the depth view, ``/admin/history`` for the long-running-jobs sort, and so
on. A playbook row whose page 404s is a dead end mid-incident. The route
table is read from the router's own source (the ``@router.get/post`` paths in
``src/taskq/web/admin/``), and every ``/admin/...`` reference in the ops and
admin-ui guides must be covered by a registered route template.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_ADMIN_DIR = _REPO_ROOT / "src" / "taskq" / "web" / "admin"
_GUIDES = (
    _REPO_ROOT / "docs" / "guides" / "ops.md",
    _REPO_ROOT / "docs" / "guides" / "admin-ui.md",
)


def _router_templates() -> set[str]:
    """Every route path the admin router registers, as written in source."""
    templates: set[str] = set()
    for py in _ADMIN_DIR.glob("*.py"):
        for match in re.finditer(
            r"""\.(?:get|post)\(\s*"([^"]+)""", py.read_text(encoding="utf-8")
        ):
            path = match.group(1)
            if path.startswith("/"):
                templates.add(path)
    # The admin factory nests the progress SSE/poll-state router at "/jobs"
    # inside the admin router (_factory.py), so its routes are read from
    # web/progress.py and joined under /jobs here.
    progress_py = _REPO_ROOT / "src" / "taskq" / "web" / "progress.py"
    for match in re.finditer(
        r"""\.(?:get|post)\(\s*"([^"]+)""", progress_py.read_text(encoding="utf-8")
    ):
        path = match.group(1)
        if path.startswith("/"):
            templates.add(f"/jobs{path}")
    return templates


def _doc_admin_refs() -> set[str]:
    """Every concrete ``/admin/<path>`` the guides point an operator at."""
    refs: set[str] = set()
    for md in _GUIDES:
        text = md.read_text(encoding="utf-8")
        for match in re.finditer(r"/admin/[a-z0-9_][a-z0-9_{}/:_.-]*", text):
            refs.add(match.group(0).rstrip("."))
    return refs


def _ref_matches_template(ref: str, template: str) -> bool:
    """Does the documented ref resolve against one route template?

    ``/admin/queues/{queue}`` matches ``/queues/{queue:path}``; concrete
    refs like ``/admin/actors`` match the same literal.
    """
    ref_tail = ref.removeprefix("/admin/")
    tpl_tail = template.removeprefix("/")
    ref_parts = ref_tail.split("/")
    tpl_parts = tpl_tail.split("/")
    if len(ref_parts) != len(tpl_parts):
        return False
    return all(tp.startswith("{") or rp == tp for rp, tp in zip(ref_parts, tpl_parts, strict=True))


def test_every_admin_route_the_ops_docs_reference_exists() -> None:
    templates = _router_templates()
    assert "/actors" in templates and "/jobs" in templates, (
        "the admin router's routes are not discoverable by the source scan - "
        "if the registration style changed (add_api_route, a factory), update "
        "this pin; do not delete it"
    )
    missing: dict[str, list[str]] = {}
    for ref in sorted(_doc_admin_refs()):
        if not any(_ref_matches_template(ref, t) for t in templates):
            missing.setdefault(ref.split("/")[1], []).append(ref)
    assert not missing, (
        f"the guides reference admin surfaces the router does not register: "
        f"{missing} - a playbook row whose page 404s is a dead end mid-incident"
    )
