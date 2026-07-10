"""Test spies for TaskQ — lightweight recording doubles."""

from __future__ import annotations

__all__ = ["WarningSpy"]


class WarningSpy:
    """Records how many times warning() was called, without sniffing args."""

    def __init__(self) -> None:
        self.warning_count = 0

    def warning(self, *_args: object, **_kwargs: object) -> None:
        self.warning_count += 1
