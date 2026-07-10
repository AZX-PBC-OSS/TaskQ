"""Shared DI internal helpers."""

from typing import get_origin

from taskq.context import JobContext


def _origin_is_job_context(annotation: object) -> bool:  # pyright: ignore[reportUnusedFunction] — used by _validate.py and registry.py via import
    """Return ``True`` if ``annotation`` resolves to ``JobContext[...]``."""
    origin = get_origin(annotation)
    if origin is JobContext:
        return True
    return annotation is JobContext
