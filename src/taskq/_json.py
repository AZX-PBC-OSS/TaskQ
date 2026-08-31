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

__all__ = ["dumps", "dumps_jsonb_str", "dumps_str", "loads", "structlog_serializer"]

# orjson renders a NUL codepoint as exactly these six characters.
_NUL_ESCAPE = "\\u0000"

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


def _has_nul(value: object, /) -> bool:
    """True when any string anywhere in *value* contains a NUL codepoint.

    Walks the value parsed back out of orjson, so only the types
    :func:`orjson.loads` can produce need handling.
    """
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        pairs = cast(dict[object, object], value)
        return any(_has_nul(k) or _has_nul(v) for k, v in pairs.items())
    if isinstance(value, list):
        entries = cast(list[object], value)
        return any(_has_nul(v) for v in entries)
    return False


def dumps_jsonb_str(value: Any, /) -> str:
    """Serialize for binding to a PostgreSQL ``jsonb`` parameter.

    Identical to :func:`dumps_str`, except a NUL (U+0000) anywhere in the
    value is rejected here rather than by the database.

    ``\\u0000`` is valid JSON and a ``json`` column stores it happily, but
    ``jsonb`` decodes to ``text``, which cannot hold a NUL, so ``jsonb_in``
    fails with SQLSTATE 22P05.  Caller-supplied payloads, metadata and actor
    results reach ``jsonb`` columns after type-and-size validation that says
    nothing about content, so without this guard a single NUL surfaces as an
    ``asyncpg.UntranslatableCharacterError`` raised from deep inside the
    INSERT: opaque to an enqueue caller, and worse on the terminal-write
    path, where ``_TERMINAL_WRITE_INFRA_EXCEPTIONS`` reads any
    ``PostgresError`` as transient infrastructure failure.  The job is then
    never marked failed; it stays ``running`` until the lease sweep reclaims
    it, re-runs, produces the same NUL, and loops, re-executing the actor's
    already-committed side effects each time.  Raising a ``ValueError`` here
    keeps that classification honest: it is a permanent data defect, so the
    normal actor-failure path handles it.

    The ``\\u0000`` substring test is only a cheap prefilter.  orjson emits the
    same six characters for the *literal* text ``\\u0000``, which ``jsonb``
    accepts, so a hit is confirmed against the parsed value before raising.
    """
    text = dumps(value).decode("utf-8")
    if _NUL_ESCAPE in text and _has_nul(orjson.loads(text)):
        raise ValueError(
            "value contains a NUL character (U+0000), which PostgreSQL cannot "
            "store in a jsonb column; strip control characters before storing"
        )
    return text


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
