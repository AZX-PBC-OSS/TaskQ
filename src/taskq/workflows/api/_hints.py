"""The body-hint resolution seam (one home): the compile's compatibility
rule (E5) and the runner's payload codec (the typed boundary) read the
SAME resolution — a body's annotations, RESOLVED (modules run under
``from __future__ import annotations``, so every annotation arrives a
string). A name the function's globals cannot resolve is looked up in the
body's CLOSURE (a locally-declared model the body captured — the wiring
shape). Unresolvable → {}: the rule that needs a type SKIPS (the
zero-false-positive doctrine — a name the compile cannot resolve is never
convicted on a guess)."""

from __future__ import annotations

import typing
from collections.abc import Callable

__all__ = ["body_hints", "inner_fn"]


def inner_fn(body: object) -> object:
    """The body's INNER function: the ``@app.actor`` canonical path wires
    the ``WorkflowActor`` HANDLE into the node (``node.body`` rides the
    wrapper — the F3 carrier), and every read that needs the body's own
    identity (the source text, the canonical hash) must unwrap it. The
    wrapper delegates attribute reads (``__getattr__`` → the inner fn),
    so the ANNOTATION reads (E4/E9's faces) work through it — but the
    SOURCE reads (``inspect.getsource``) refuse an instance outright
    ("code object was expected, got WorkflowActor" — the stamp loss the
    rv3 teardown round convicted). This seam is the ONE unwrap: the
    validator's static walk and the runner's code-version stamper read
    through it, never around it."""
    from taskq.workflows.api._app import WorkflowActor

    if isinstance(body, WorkflowActor):
        return body.fn
    return body


def body_hints(body: Callable[..., object]) -> dict[str, object]:
    """The body's resolved annotation hints (see the module docstring)."""
    try:
        return dict(typing.get_type_hints(body))
    except NameError:
        localns: dict[str, object] = {}
        for cell in getattr(body, "__closure__", None) or ():
            try:
                value = cell.cell_contents
            except ValueError:  # pragma: no cover - an emptied cell
                continue
            name = getattr(value, "__name__", None)
            if isinstance(name, str):
                localns[name] = value
        if not localns:
            return {}
        try:
            return dict(typing.get_type_hints(body, localns=localns))
        except Exception:
            return {}
    except Exception:
        return {}
