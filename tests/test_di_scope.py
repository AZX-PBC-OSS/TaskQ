"""Unit tests for Scope IntEnum and LifecycleDetectionWarning."""

from pathlib import Path

import pytest

from taskq._di.scope import LifecycleDetectionWarning, Scope

_REPO_ROOT = Path(__file__).parent.parent
_SCOPE_PY = _REPO_ROOT / "src/taskq/_di/scope.py"
_INIT_PY = _REPO_ROOT / "src/taskq/_di/__init__.py"
_DI_PY = _REPO_ROOT / "src/taskq/di.py"

_FORBIDDEN_IMPORT = "from __future__ import annotations"


# ── Scope enum membership and ordering ──────────────────────────────


def test_scope_has_four_members() -> None:
    """Scope has exactly PROCESS, THREAD, LOOP, TRANSIENT."""
    assert list(Scope) == [Scope.PROCESS, Scope.THREAD, Scope.LOOP, Scope.TRANSIENT]


def test_scope_integer_values() -> None:
    """Integer values are exactly 0, 1, 2, 3."""
    assert [s.value for s in Scope] == [0, 1, 2, 3]


def test_scope_ordering() -> None:
    """PROCESS < THREAD < LOOP < TRANSIENT."""
    assert Scope.PROCESS < Scope.THREAD < Scope.LOOP < Scope.TRANSIENT


def test_scope_dict_key_round_trip() -> None:
    """Scope members are usable as dict keys."""
    d = {Scope.LOOP: 1}
    assert d[Scope.LOOP] == 1


def test_scope_isinstance_int() -> None:
    """Scope members are int instances and support arithmetic."""
    assert isinstance(Scope.LOOP, int)
    assert Scope.LOOP + 1 == 3


# ── LifecycleDetectionWarning ──────────────────────────────────────


def test_lifecycle_detection_warning_is_user_warning() -> None:
    """LifecycleDetectionWarning is subclass of UserWarning and Warning."""
    assert issubclass(LifecycleDetectionWarning, UserWarning)
    assert issubclass(LifecycleDetectionWarning, Warning)


# ── Public re-export wiring ─────────────────────────────────────────


def test_public_di_import() -> None:
    """from taskq.di import Scope, LifecycleDetectionWarning succeeds."""
    import taskq.di as di_mod

    assert di_mod.Scope is Scope
    assert di_mod.LifecycleDetectionWarning is LifecycleDetectionWarning


# ── No from __future__ import annotations ───────────────────────────


@pytest.mark.parametrize("path", [_SCOPE_PY, _INIT_PY, _DI_PY])
def test_no_future_annotations(path: Path) -> None:
    """Files must not contain 'from __future__ import annotations'."""
    assert _FORBIDDEN_IMPORT not in path.read_text()


# ── DIError exception surface ───────────────────────────────────────────────


def test_di_error_is_taskq_error() -> None:
    """DIError is a TaskQError subclass available from taskq top-level."""
    from taskq import DIError, TaskQError

    assert issubclass(DIError, TaskQError)
