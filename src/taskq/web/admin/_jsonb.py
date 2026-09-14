"""Canonical jsonb column decoder for asyncpg Records."""

from typing import Any

from taskq._json import loads


def decode_jsonb(value: Any) -> Any:
    """Decode a jsonb column value from an asyncpg Record to a Python object.

    asyncpg may return jsonb as a text string (default codec) or a dict
    (custom codec).  This helper normalises both paths so Jinja2 template
    tests like ``is mapping`` and attribute access work correctly.

    Parsing goes through :mod:`taskq._json` — the project never imports
    stdlib ``json`` directly. orjson's ``JSONDecodeError`` subclasses
    ``ValueError``, so the malformed-text fallback contract is unchanged.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value  # pyright: ignore[reportUnknownVariableType]  # Why: value originates from an untyped asyncpg Record; isinstance narrowing at runtime ensures correct types.
    if isinstance(value, str):
        try:
            return loads(value)
        except ValueError:
            return value
    return value
