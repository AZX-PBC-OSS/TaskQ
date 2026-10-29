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

import ast
import inspect
import re
import textwrap
import tokenize
import typing
from collections.abc import Callable
from typing import cast, overload

__all__ = ["body_hints", "ctx_input_reads", "inner_fn", "own_source"]


@overload
def inner_fn(body: Callable[..., object]) -> Callable[..., object]: ...


@overload
def inner_fn(body: object) -> object: ...


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


def own_source(body: object) -> str | None:
    """The body's OWN source text, VERIFIED (the source reads' one home):
    ``inspect.getsource`` unwrapped (:func:`inner_fn`), dedented — and
    the slice PROVEN to be this function's.

    THE SLICE-IDENTITY GUARD (the rv3 teardown round's doc-fence
    conviction): a body defined by ``exec(code, FENCE_PATH)`` (the docs
    fences) carries the .md's filename and a ``co_firstlineno`` in the
    .md's OWN line numbering, not the extracted code's — the re-sliced
    text is PROSE (a one-line fragment, the bash fence's text): the
    wrong slice either fails the tokenizer loudly or — the dangerous
    half — PARSES as a fragment nobody can reproduce. The guard demands
    the slice actually DECLARE the function's name; anything else
    returns ``None`` (the unreadable source — the caller SKIPS: a guess
    is never convicted, a prose stamp is never written)."""
    fn = inner_fn(body)
    try:
        # The callable-shaped cast IS the seam's boundary: the source
        # reader refuses every non-sourceable shape (builtin, partial,
        # the wrapper) — the TypeError IS the skip's face.
        source = inspect.getsource(cast("Callable[..., object]", fn))
        dedented = textwrap.dedent(source)
        name = getattr(fn, "__name__", "")
        if not name or not re.search(
            rf"^\s*(?:async\s+)?def\s+{re.escape(name)}\s*\(", dedented, re.MULTILINE
        ):
            return None
        ast.parse(
            dedented
        )  # the parse check rides the read (the TokenError face — getblock re-tokenizes the slice)
        return dedented
    except (
        OSError,
        TypeError,
        ValueError,
        SyntaxError,
        tokenize.TokenError,
    ):
        return None


def ctx_input_reads(body: object) -> bool | None:
    """THE BODY'S STATIC ``ctx.input`` READ (the cross-run cache's
    address-scope fence): an AST walk over the body's own source —
    ``True`` when any ``<ctx>.input`` attribute chain appears (the ctx
    parameter's own name read first: the runner invokes the body
    ``body(ctx, …)`` first-positional), ``False`` when none does,
    ``None`` when the source is NOT readable (a builtin, a partial, an
    exec'd fragment — the caller SKIPS: the zero-false-positive
    doctrine, a guess is never convicted).

    THE SCAN IS CRUDE, ON PURPOSE (the declared boundary): it reads the
    body's OWN source only — a helper the body calls may read
    ``ctx.input`` unseen (the same closure boundary the address's
    helper-closure law states), and an aliased ctx (``c = ctx`` then
    ``c.input``) reads unseen. The wiring refusal names the cure
    regardless of the scan's blind spots: wire the value as a PARAM so
    it enters the address. Both the wiring verb (:func:`taskq.workflows.step`)
    and the validator's E18 re-proof read THIS walk — one home."""
    source = own_source(body)
    if source is None:
        return None
    fn = inner_fn(body)
    ctx_name = "ctx"
    try:
        sig = inspect.signature(cast("Callable[..., object]", fn))
    except (TypeError, ValueError):
        pass
    else:
        params = [
            p
            for p in sig.parameters.values()
            if p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]
        if params:
            ctx_name = params[0].name
    found = False

    def walk(node: ast.AST) -> None:
        nonlocal found
        if isinstance(node, ast.Attribute) and node.attr == "input":
            value = node.value
            if isinstance(value, ast.Name) and value.id == ctx_name:
                found = True
        for child in ast.iter_child_nodes(node):
            walk(child)

    walk(ast.parse(source))
    return found


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
