"""orjson-backed JSON helpers.

The library never imports stdlib ``json`` directly. Use ``dumps`` / ``loads``
from this module so behaviour is consistent and the serialization hot path
stays fast.

``loads`` does NOT revive UUID-like strings into :class:`uuid.UUID` objects.
Type coercion is the responsibility of the consuming Pydantic model
(``model_validate`` coerces strings to ``UUID`` when the field is typed
``UUID``, and keeps them as ``str`` when the field is typed ``str``).
This respects the principle of least surprise: the developer's declared
field type is the source of truth, not the deserializer's guess.
"""

from __future__ import annotations

from typing import Any

import orjson

__all__ = ["dumps", "dumps_str", "loads", "structlog_serializer"]


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

    Returns plain Python types (str, int, float, bool, None, list, dict).
    UUID-like strings remain ``str`` — the consuming Pydantic model's
    ``model_validate`` coerces them to ``UUID`` when the field is typed
    ``UUID``, and keeps them as ``str`` when the field is typed ``str``.
    """
    return orjson.loads(data)


def structlog_serializer(value: Any, /, **_kwargs: Any) -> str:
    """Serialize to ``str`` for structlog's ``JSONRenderer(serializer=...)``.

    Accepts and ignores ``**_kwargs`` (e.g. ``default``) that structlog passes
    internally — orjson handles all types we encounter natively and does not
    use the ``default`` fallback that stdlib ``json`` requires.
    """
    return dumps_str(value)
