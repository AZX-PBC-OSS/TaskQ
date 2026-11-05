"""The shared SQL render: one token-substitution pass for every
statement family that renders a named constant template per schema.

Leaf module by design (mirrors ``taskq._shield``): imports only
``functools`` and the schema validator, so the backend, the workflows
engine, the worker's loops and the progress flush can all depend on it
without a layering inversion.

The substitution discipline every family already followed (the
``WorkflowSql.build`` law): ``{schema}`` is the universal token and the
ONLY caller-derived value, validated against the estate's ``_IDENT_RE``
here before anything renders; *subs* carries each family's extra
fixed-vocabulary tokens (the terminal set, the priority stamp's
pattern, the wf-cancel error class), the engine's own constants, never
caller input, applied after the schema as a plain ``str.replace`` of an
exact token each — the templates are never ``.format``\\ted, so a
jsonb shape literal's bare braces survive (the JSONB-landmine rule).
*collapse_braces* un-doubles ``{{``/``}}`` for the templates that carry
the migration-runner convention and need them single at execution.

The render is memoised per ``(template, schema, subs, collapse)`` —
keyed on the template STRING, never a bundle object: the statement
constants are module-level literals, so one schema's whole working set
(the engine bundle's ~57 statements plus the loop, runner, rootless
sweep, hitl, cron and flush families — roughly ninety renders) is
substituted once per process and every later build is a cache hit; the
leader's sweep arms re-render the ~65 KiB bundle per arm, and the warm
path pays about a tenth of a microsecond per render (measured: 111 ns
median, a whole bundle build 10.3 µs — the leader's per-arm cost). The
maxsize holds several schemas' working sets (a process serves one;
the test suite mounts a few). An invalid schema raises the same
ValueError on every call — the cache never serves an unvalidated
render.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Final

from taskq.constants import require_schema

__all__ = ["render"]

#: The cache's size: one schema's whole statement working set is a few
#: hundred renders at most (see the module docstring), so a handful of
#: schemas fit without evicting a live family's entries.
_RENDER_CACHE_MAXSIZE: Final[int] = 256


@lru_cache(maxsize=_RENDER_CACHE_MAXSIZE)
def _render_cached(
    template: str,
    schema: str,
    subs: tuple[tuple[str, str], ...],
    collapse_braces: bool,
) -> str:
    require_schema(schema)
    out = template.replace("{schema}", schema)
    for token, value in subs:
        out = out.replace(token, value)
    # The doubled braces in the jsonb shape literals are the
    # migration-runner convention; substitution needs them single.
    if collapse_braces:
        out = out.replace("{{", "{").replace("}}", "}")
    return out


def render(
    template: str,
    schema: str,
    *,
    subs: tuple[tuple[str, str], ...] = (),
    collapse_braces: bool = False,
) -> str:
    """Render *template* for *schema*: the validated schema identifier
    substituted for ``{schema}`` first, then *subs*' extra tokens in
    order, then (when *collapse_braces*) the doubled braces un-doubled.

    The same inputs return the same SQL — the render is memoised on the
    template string's value (see the module docstring), so a caller that
    renders a module constant repeatedly pays the substitution once.
    """
    return _render_cached(template, schema, subs, collapse_braces)
