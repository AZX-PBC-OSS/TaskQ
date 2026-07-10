"""orjson-backed JSON helpers.

The library never imports stdlib ``json`` directly. Use ``dumps`` / ``loads``
from this module so behaviour is consistent (UUID, datetime, numpy support
where compiled) and the serialization hot path stays fast.
"""

from __future__ import annotations

import re
from typing import Any, cast
from uuid import UUID

import orjson

__all__ = ["dumps", "dumps_str", "loads", "structlog_serializer"]

_UUID_RE = re.compile(
    r"\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z",
    re.IGNORECASE,
)


def _revive_uuids(value: Any) -> Any:
    """Recursively convert UUID-like strings back to :class:`uuid.UUID`.

    orjson serializes :class:`uuid.UUID` to the canonical 36-character hex
    string but deserializes it back as a plain ``str``.  This helper walks
    dict values and list items (not dict keys) and converts any string that
    looks like a UUID back to a :class:`uuid.UUID` instance so that the
    round-trip through PostgreSQL jsonb columns is transparent.
    """
    if isinstance(value, str):
        if len(value) == 36 and _UUID_RE.match(value):
            try:
                return UUID(value)
            except ValueError:
                pass
        return value
    if isinstance(value, dict):
        d = cast(dict[object, Any], value)
        return {k: _revive_uuids(v) for k, v in d.items()}
    if isinstance(value, list):
        lst = cast(list[Any], value)
        return [_revive_uuids(v) for v in lst]
    return value


def _orjson_fallback(obj: Any) -> Any:
    """Convert types orjson can't serialize natively to a JSON-safe form.

    Only reached when *obj* is not a type orjson handles (UUID, datetime,
    str, int, float, bool, None, list, dict).  Kept fast-path: the vast
    majority of values never hit this function.
    """
    cls: type[Any] = type(obj)  # pyright: ignore[reportUnknownVariableType]  # Why: obj is Any from orjson's default function; type() always returns a valid type object.
    mod = cls.__module__
    name = cls.__qualname__

    # asyncpg protocol-level UUID — raw record access can leak these into
    # structlog event dicts; convert to standard UUID string form.
    if mod.startswith("asyncpg") and "UUID" in name:
        return str(obj)

    # bytes in a log event dict — decode with replacement.
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="replace")

    raise TypeError(f"Type is not JSON serializable: {mod}.{name}")


def dumps(value: Any, /) -> bytes:
    """Serialize to bytes. Uses orjson defaults (UTC datetimes, UUID, etc.)."""
    return orjson.dumps(
        value,
        default=_orjson_fallback,
        option=orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z | orjson.OPT_NON_STR_KEYS,
    )


def dumps_str(value: Any, /) -> str:
    """Serialize to ``str``. Use only when the consumer demands text (e.g.,
    asyncpg jsonb codec). Prefer :func:`dumps` for everything else."""
    return dumps(value).decode("utf-8")


def loads(data: bytes | bytearray | memoryview | str, /) -> Any:
    """Deserialize bytes or text to a Python value.

    UUID strings produced by :func:`dumps` / :func:`dumps_str` are revived
    back to :class:`uuid.UUID` instances so that PostgreSQL jsonb columns,
    NOTIFY payloads, event details, and other JSON-serialised state round-trip
    correctly.
    """
    return _revive_uuids(orjson.loads(data))


def structlog_serializer(value: Any, /, **_kwargs: Any) -> str:
    """Serialize to ``str`` for structlog's ``JSONRenderer(serializer=...)``.

    Accepts and ignores ``**_kwargs`` (e.g. ``default``) that structlog passes
    internally — orjson handles all types we encounter natively and does not
    use the ``default`` fallback that stdlib ``json`` requires.
    """
    return dumps_str(value)
