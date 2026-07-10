"""Tests for the ``memory_jobs`` fixture and taskq.testing public surface.

``taskq.backend`` must NOT expose FakeClock, InMemoryBackend,
run_until_drained, or tick_cancel_polling. ``taskq.testing`` must
expose these names.

``taskq.testing`` must not transitively import asyncpg, redis,
or testcontainers. Verified via subprocess to avoid polluting the
running test process's ``sys.modules``.

Sanity: ``memory_jobs`` yields an InMemoryBackend whose clock starts at
``datetime(2025, 1, 1, tzinfo=UTC)``.
"""

import subprocess
import sys

import pytest

from taskq.testing import InMemoryBackend
from taskq.testing.clock import FakeClock as _FakeClock
from taskq.testing.in_memory import InMemoryBackend as _InMemoryBackend
from taskq.testing.job_context import JobContext as _JobContext

# ── Sanity: memory_jobs yields InMemoryBackend with correct clock ──────


@pytest.mark.asyncio
async def test_memory_jobs_yields_in_memory_backend(memory_jobs: InMemoryBackend) -> None:
    """sanity: memory_jobs yields an InMemoryBackend."""
    assert isinstance(memory_jobs, InMemoryBackend)


@pytest.mark.asyncio
async def test_memory_jobs_clock_starts_at_epoch(memory_jobs: InMemoryBackend) -> None:
    """sanity: clock.now() == datetime(2025, 1, 1, tzinfo=UTC)."""
    from datetime import UTC, datetime

    assert memory_jobs._clock.now() == datetime(2025, 1, 1, tzinfo=UTC)  # type: ignore[reportPrivateUsage]


# ── taskq.backend must NOT expose test-only names ─────────────


def test_backend_does_not_expose_fake_clock() -> None:
    """FakeClock is NOT accessible from taskq.backend."""
    import taskq.backend

    assert not hasattr(taskq.backend, "FakeClock")


def test_backend_does_not_expose_in_memory_backend() -> None:
    """InMemoryBackend is NOT accessible from taskq.backend."""
    import taskq.backend

    assert not hasattr(taskq.backend, "InMemoryBackend")


def test_backend_does_not_expose_run_until_drained() -> None:
    """run_until_drained is NOT accessible from taskq.backend."""
    import taskq.backend

    assert not hasattr(taskq.backend, "run_until_drained")


def test_backend_does_not_expose_tick_cancel_polling() -> None:
    """tick_cancel_polling is NOT accessible from taskq.backend."""
    import taskq.backend

    assert not hasattr(taskq.backend, "tick_cancel_polling")


def test_testing_exposes_fake_clock() -> None:
    """FakeClock IS accessible from taskq.testing."""
    import taskq.testing

    assert hasattr(taskq.testing, "FakeClock")
    assert taskq.testing.FakeClock is _FakeClock


def test_testing_exposes_in_memory_backend() -> None:
    """InMemoryBackend IS accessible from taskq.testing."""
    import taskq.testing

    assert hasattr(taskq.testing, "InMemoryBackend")
    assert taskq.testing.InMemoryBackend is _InMemoryBackend


def test_testing_exposes_job_context() -> None:
    """JobContext IS accessible from taskq.testing."""
    import taskq.testing

    assert hasattr(taskq.testing, "JobContext")
    assert taskq.testing.JobContext is _JobContext


# ── taskq.testing must not transitively import heavy deps ────


def test_testing_no_transitive_asyncpg() -> None:
    """importing taskq.testing does not pull in asyncpg.

    Verified via subprocess so the running test process's sys.modules
    is not polluted (research Approach A).
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import taskq.testing; import sys; "
            "assert 'asyncpg' not in sys.modules, 'asyncpg leaked into sys.modules'",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_testing_no_transitive_redis() -> None:
    """importing taskq.testing does not pull in redis."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import taskq.testing; import sys; "
            "assert 'redis' not in sys.modules, 'redis leaked into sys.modules'",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_testing_no_transitive_testcontainers() -> None:
    """importing taskq.testing does not pull in testcontainers."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import taskq.testing; import sys; "
            "assert 'testcontainers' not in sys.modules, 'testcontainers leaked into sys.modules'",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


# ── Regression: backend_pair pg param must not boot containers in unit tier ──


@pytest.mark.asyncio
async def test_backend_pair_memory_without_integration_marker(
    backend_pair: object,
) -> None:
    """Regression (findings-1 Warning 1): backend_pair's memory param
    works without ``@pytest.mark.integration``. The pg param is
    automatically skipped via ``pytest.skip`` so the unit tier stays
    container-free.

    This test is intentionally NOT marked ``@pytest.mark.integration``.
    When collected, pytest will produce two variants:
    - ``memory``: passes (runs this body).
    - ``pg``: SKIPPED (the guard in the fixture fires).
    """
    from taskq.testing.in_memory import InMemoryBackend

    assert isinstance(backend_pair, InMemoryBackend)
