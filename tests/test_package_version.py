"""The version pin: the runtime `__version__` is the installed
distribution's version, in both directions."""

import importlib.metadata

import taskq


def test_dunder_version_is_the_installed_distribution() -> None:
    assert taskq.__version__ == importlib.metadata.version("taskq-py")


def test_dunder_version_is_public_surface() -> None:
    assert "__version__" in taskq.__all__
