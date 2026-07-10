"""Canonical jsonb column decoder for asyncpg Records."""

import json
from typing import Any


def decode_jsonb(value: Any) -> Any:
    """Decode a jsonb column value from an asyncpg Record to a Python object.

    asyncpg may return jsonb as a text string (default codec) or a dict
    (custom codec).  This helper normalises both paths so Jinja2 template
    tests like ``is mapping`` and attribute access work correctly.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value  # pyright: ignore[reportUnknownVariableType]  # Why: value originates from an untyped asyncpg Record; isinstance narrowing at runtime ensures correct types.
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value
