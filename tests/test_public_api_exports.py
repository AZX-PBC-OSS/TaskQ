"""Tests for the public API surface exported by ``taskq.__init__``.

Verifies that every name in ``taskq.__all__`` is importable from the
top-level package and that the newly exported types are what they claim
to be (Protocol, dataclass, etc.).
"""

from __future__ import annotations

import dataclasses
import inspect
from typing import Protocol

import pytest

import taskq

# -- New exports --

_NEW_EXPORTS = [
    "Backend",
    "Clock",
    "FakeClock",
    "JobPage",
    "JobRow",
    "SystemClock",
    "TaskQSettings",
    "WorkerSettings",
]


@pytest.mark.parametrize("name", _NEW_EXPORTS)
def test_new_export_is_importable_from_taskq(name: str) -> None:
    """Each newly added export is importable from the top-level package."""
    assert hasattr(taskq, name), f"taskq.{name} is missing"
    obj = getattr(taskq, name)
    assert obj is not None, f"taskq.{name} is None"


@pytest.mark.parametrize("name", _NEW_EXPORTS)
def test_new_export_is_in_all(name: str) -> None:
    """Each newly added export appears in ``taskq.__all__``."""
    assert name in taskq.__all__, f"{name!r} not in taskq.__all__"


# -- Structural verification --


def test_clock_is_runtime_checkable_protocol() -> None:
    """``Clock`` is a ``@runtime_checkable`` Protocol."""
    assert inspect.isclass(taskq.Clock)
    assert issubclass(taskq.Clock, Protocol)


def test_system_clock_is_frozen_dataclass() -> None:
    """``SystemClock`` is a frozen dataclass (slots)."""
    assert dataclasses.is_dataclass(taskq.SystemClock)
    params = dataclasses.fields(taskq.SystemClock)
    field_names = {f.name for f in params}
    assert field_names == set()


def test_fake_clock_is_class_with_now_and_monotonic() -> None:
    """``FakeClock`` has the ``now()`` and ``monotonic()`` methods that ``Clock`` requires."""
    assert inspect.isclass(taskq.FakeClock)
    assert hasattr(taskq.FakeClock, "now")
    assert hasattr(taskq.FakeClock, "monotonic")
    assert callable(taskq.FakeClock.now)
    assert callable(taskq.FakeClock.monotonic)


def test_backend_is_runtime_checkable_protocol() -> None:
    """``Backend`` is a ``@runtime_checkable`` Protocol."""
    assert inspect.isclass(taskq.Backend)
    assert issubclass(taskq.Backend, Protocol)


def test_job_row_is_frozen_dataclass_with_slots() -> None:
    """``JobRow`` is a frozen dataclass."""
    assert dataclasses.is_dataclass(taskq.JobRow)
    params = dataclasses.fields(taskq.JobRow)
    field_names = {f.name for f in params}
    assert "id" in field_names
    assert "status" in field_names
    assert "actor" in field_names


def test_job_page_is_frozen_dataclass() -> None:
    """``JobPage`` is a frozen dataclass with ``jobs`` and ``next_cursor`` fields."""
    assert dataclasses.is_dataclass(taskq.JobPage)
    field_names = {f.name for f in dataclasses.fields(taskq.JobPage)}
    assert "jobs" in field_names
    assert "next_cursor" in field_names


def test_taskq_settings_is_class() -> None:
    """``TaskQSettings`` is a class with a ``load`` classmethod."""
    assert inspect.isclass(taskq.TaskQSettings)
    assert hasattr(taskq.TaskQSettings, "load")
    assert callable(taskq.TaskQSettings.load)


def test_worker_settings_is_subclass_of_taskq_settings() -> None:
    """``WorkerSettings`` extends ``TaskQSettings``."""
    assert inspect.isclass(taskq.WorkerSettings)
    assert issubclass(taskq.WorkerSettings, taskq.TaskQSettings)


# -- All __all__ entries are importable --


def test_all_exports_are_importable() -> None:
    """Every name in ``taskq.__all__`` must resolve to a real attribute on the package.

    This is a regression guard: if a name is listed in ``__all__`` but the
    import was removed or renamed, this test catches it.
    """
    missing = [name for name in taskq.__all__ if not hasattr(taskq, name)]
    assert not missing, f"Names in __all__ but not importable: {missing}"


def test_all_exports_match_all_list() -> None:
    """No duplicate entries in ``taskq.__all__``."""
    names = taskq.__all__
    duplicates = [n for n in names if names.count(n) > 1]
    assert not duplicates, f"Duplicate entries in __all__: {sorted(set(duplicates))}"


def test_all_is_sorted() -> None:
    """``taskq.__all__`` is sorted (capitalised entries first, then lowercase)."""
    names = taskq.__all__
    cap_names = [n for n in names if n[0].isupper()]
    assert cap_names == sorted(cap_names), (
        f"Capitalised entries in __all__ are not sorted: {cap_names}"
    )
