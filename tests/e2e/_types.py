"""Structural types for e2e fixtures backed by e2e-only dependencies.

``containerspec`` (the worker-image builder) lives in the ``e2e``
dependency group on purpose -- the default dev environment and the
non-e2e CI legs never install the docker SDK (see ``[dependency-groups]``
in pyproject.toml), so the package's types are unresolvable exactly where
pyright runs. The suite consumes exactly one attribute of the built image
(``.tag``); this Protocol names that surface without importing the
package, keeping fixture annotations resolvable in every environment.
Runtime objects are the real ``containerspec.BuiltImage``; the read-only
property shape accepts plain attributes and frozen-dataclass fields
alike, so the structural match still holds when the e2e group is synced.
"""

from typing import Protocol


class BuiltImage(Protocol):
    """The worker image built once per session by the ``e2e_worker_image`` fixture."""

    @property
    def tag(self) -> str: ...
