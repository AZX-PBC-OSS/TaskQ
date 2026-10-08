"""The flow RUNNER's row-codec seam (the split of ``api/_runner``): the
jsonb envelope's boundary — every value that crosses a row (a wiring
arg, a body return, the loop's carry, the flow's input) walks these
typed helpers, decoded ONCE (cut #14), encoded jsonb-safe.

The envelope's law: a body's bare dict return never collides with the
engine's envelope keys (the ``{"value": …}`` wrapper); the Any-contract
walk is the house style ``_types.py``'s decoders use — every branch
asserts the runtime shape it consumes, and each USE of an Unknown member
carries the targeted ignore with the Why.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel, TypeAdapter

from taskq._json import loads as _json_loads

__all__ = ["FlowEntryShim"]


@dataclass(slots=True)
class FlowEntryShim:
    """The FlowEntry shape the run-key claim needs, built from the
    compiled workflow (the typed door's runtime form). Mutable: the
    protocol's members are writable (the estate's carrier convention)."""

    name: str
    payload: dict[str, object] | str | None
    actor: str = "wf"
    queue: str = "default"
    max_attempts: int = 3
    retry_kind: str = "transient"
    trace_id: str | None = None


def next_data(iterator: Any) -> object:
    """One data-arg position's value (the payload's wf_args array is the
    wiring's recorded data, in order)."""
    return cast(object, next(iterator))


def encode_data_arg(value: object) -> object:
    """The jsonb-safe form of one wiring value (a pydantic model dumps
    through its own codec — the typed boundary; lists and dicts walk).
    Everything else is already a JSON value by the wiring's contract."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        items = cast(list[object], value)  # the walk's declared members
        return [encode_data_arg(v) for v in items]
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)  # the walk's declared members
        walked_map: dict[str, object] = {}
        for k, v in mapping.items():
            walked_map[str(k)] = encode_data_arg(v)
        return walked_map
    return value


def jsonable(value: object) -> object:
    """The carry/feedback's jsonb-safe form (the walk's boundary)."""
    return encode_data_arg(value)


def encode_result(value: object) -> dict[str, object] | None:
    """The result envelope: the body's return rides the row's jsonb
    ``result`` (wrapped so a bare dict body return never collides with
    the engine's envelope keys). The value walks the typed boundary (a
    body may return a list of models — the map's shape)."""
    if value is None:
        return None
    return {"value": encode_data_arg(value)}


def decode_result(raw: object) -> object:
    """jsonb decoded ONCE (cut #14): asyncpg returns ``str`` on un-coded
    connections — parse through the estate's seam, never per call site.
    The runner's own envelope (a bare ``{"value": …}``) unwraps — a body's
    dict return never collides with the envelope keys."""
    if raw is None:
        return None
    # The Any-contract walk (the _json seam's parse contract — the same
    # house style _types.py's decoders use): every branch asserts the
    # runtime shape it consumes, and each USE of an Unknown member carries
    # the targeted ignore with the Why.
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    if "value" in decoded and len(decoded) == 1:  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk.
        return cast(object, decoded["value"])  # pyright: ignore[reportUnknownArgumentType]  # Why: the walk's boundary — the cast IS the declared laundering (the envelope's single key is the runner's own).
    return cast(object, decoded)  # pyright: ignore[reportUnknownArgumentType]  # Why: the same boundary.


# ── THE TYPED BOUNDARY IS COMPLETE (the phase-4 cure): a param's
# annotation — bare model, LIST, union, generic — re-validates the jsonb
# round-trip's value. The collect's dicts reached the body RAW and the
# union's `match` fell to the never-arm (the doc-ingest fence's route
# failure); the walk below closes it. The adapters cache per annotation —
# the module-level bodies' annotations are immortal.

_TYPE_ADAPTERS: dict[object, TypeAdapter[object]] = {}  # pyright: ignore[reportUnknownVariableType]  # Why: TypeAdapter's generic for arbitrary annotations is partially unknown by construction; the cache IS the boundary.


def type_adapter_for(param: object) -> Any:
    """The annotation's TypeAdapter, cached. The ANY return is the
    boundary's declared contract (TypeAdapter's generic for arbitrary
    annotations is partially unknown by construction — the cache IS the
    seam)."""
    adapter = _TYPE_ADAPTERS.get(param)
    if adapter is None:
        built: Any = TypeAdapter(param)  # pyright: ignore[reportUnknownVariableType]  # Why: TypeAdapter's generic for arbitrary annotations is partially unknown by construction; the cache IS the seam.
        _TYPE_ADAPTERS[param] = built
        adapter = built
    return adapter


def coerce_arg(
    raw: Any,
    *,
    param: Any,
    position: int,
    params: list[object],
) -> object:
    # The ANY params are the walk's declared contract: the caller's
    # body_hints walk is the Any-contract's boundary (the annotations are
    # objects); the RETURN is the typed door's product.
    """The payload codec walk: the body's declared param annotation
    re-validates the jsonb round-trip's value (the decode once, typed
    end to end). A BARE model is the fast path; a LIST/union/generic
    annotation walks the TypeAdapter."""
    if param is object or param is Any:
        return raw
    if isinstance(param, type) and issubclass(param, BaseModel):
        if isinstance(raw, dict):
            return param.model_validate(raw)
        return raw  # the non-dict JSON values need no codec
    adapter = type_adapter_for(cast("object", param))
    raw_arg: Any = raw
    validated: object = adapter.validate_python(raw_arg)  # pyright: ignore[reportUnknownMemberType]  # Why: the cached boundary (the annotation walk); the Any-typed walk is the codec's declared contract.
    return validated
