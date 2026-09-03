"""Tests for the public API surface exported by ``taskq.__init__``.

Verifies that every name in ``taskq.__all__`` is importable from the
top-level package and that the newly exported types are what they claim
to be (Protocol, dataclass, etc.).
"""

from __future__ import annotations

import dataclasses
import inspect
from typing import Protocol, get_args

import pytest

import taskq
import taskq.backend

# -- New exports --

_NEW_EXPORTS = [
    "Backend",
    "Clock",
    "FakeClock",
    "JobPage",
    "JobRow",
    "JobStatus",
    "OIDCSettings",
    "SAMLSettings",
    "SystemClock",
    "TERMINAL_STATUSES",
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


def test_oidc_settings_is_class() -> None:
    """``OIDCSettings`` is a class with a ``load`` classmethod."""
    assert inspect.isclass(taskq.OIDCSettings)
    assert hasattr(taskq.OIDCSettings, "load")
    assert callable(taskq.OIDCSettings.load)


def test_saml_settings_is_class() -> None:
    """``SAMLSettings`` is a class with a ``load`` classmethod."""
    assert inspect.isclass(taskq.SAMLSettings)
    assert hasattr(taskq.SAMLSettings, "load")
    assert callable(taskq.SAMLSettings.load)


def test_terminal_statuses_is_five_status_frozenset() -> None:
    """``TERMINAL_STATUSES`` is the frozenset of the five terminal statuses."""
    assert isinstance(taskq.TERMINAL_STATUSES, frozenset)
    assert (
        frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})
        == taskq.TERMINAL_STATUSES
    )


def test_terminal_statuses_is_backend_reexport() -> None:
    """``taskq.TERMINAL_STATUSES`` IS ``taskq.backend.TERMINAL_STATUSES`` —
    the same object, not an equal copy. Identity, not ``==``: a hand-copied
    frozenset that has drifted in lockstep with nothing passes ``==`` and
    defeats the drift pin — the exact failure mode issue #91 exists to
    prevent."""
    assert taskq.TERMINAL_STATUSES is taskq.backend.TERMINAL_STATUSES


def test_job_status_is_backend_type_alias() -> None:
    """``taskq.JobStatus`` is the same alias object as ``taskq.backend.JobStatus``."""
    assert taskq.JobStatus is taskq.backend.JobStatus


def test_job_status_enumerates_eight_statuses() -> None:
    """``JobStatus``'s Literal carries exactly the eight job statuses."""
    assert set(get_args(taskq.JobStatus.__value__)) == {
        "pending",
        "scheduled",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "crashed",
        "abandoned",
    }


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
    """``taskq.__all__`` is sorted isort-style, matching RUF022: SCREAMING_SNAKE
    constants first, then capitalised names, then the rest — each group held to
    code-point order.

    Classification strips leading underscores before deciding the group
    (RUF022 puts ``_``-prefixed names in their stripped core's group —
    ``_PRIVATE`` sorts with the constants, verified against ruff itself),
    so a future underscore-prefixed export cannot create a false conflict
    between this test and the lint.

    Caveat: within groups RUF022 actually uses digit-aware natural sort, which
    diverges from code-point order for digit-containing names (it orders
    "Item2" before "Item10" where sorted() demands the opposite); the digit
    guard below turns any such export into a conscious decision instead of a
    silent conflict between this test and the lint.

    The first constant (TERMINAL_STATUSES, issue #91) exposed that the old
    cap-subsequence check and this one are incomparable: it both missed
    lowercase misplacement and rejected RUF022-legal group orderings (e.g.
    VALID_TRANSITIONS before AttemptOutcome in taskq.backend.__all__).
    """
    names = taskq.__all__

    def _core(n: str) -> str:
        return n.lstrip("_")

    constants = [n for n in names if _core(n).isupper()]
    classes = [n for n in names if not _core(n).isupper() and _core(n)[:1].isupper()]
    rest = [n for n in names if not _core(n)[:1].isupper()]
    assert names == constants + classes + rest, (
        "__all__ must list constants first, then capitalised names, then the rest"
    )
    assert constants == sorted(constants), (
        f"Constant entries in __all__ are not sorted: {constants}"
    )
    assert classes == sorted(classes), f"Capitalised entries in __all__ are not sorted: {classes}"
    assert rest == sorted(rest), f"Remaining entries in __all__ are not sorted: {rest}"
    assert not any(ch.isdigit() for name in names for ch in name), (
        "export names contain digits; this test's code-point grouping must be "
        "updated to RUF022's natural sort"
    )
