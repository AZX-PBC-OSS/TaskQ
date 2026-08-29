"""Unit tests for the e2e SSE admin-server shutdown helper (container-free).

``_shutdown_admin`` lives at module level in ``tests/e2e/test_progress.py``
(deliberately importable; the e2e lane itself needs Docker). These tests pin
the liveness-guard polarity against real spawned child processes — an
inverted ``poll()`` guard terminates only already-dead children and lets live
ones eat the full ``communicate(timeout=10)`` before SIGKILL, on every
teardown.
"""

import signal
import subprocess
import sys
import time

from tests.e2e.test_progress import _shutdown_admin


def _spawn(args: list[str]) -> subprocess.Popen[str]:
    return subprocess.Popen(  # noqa: S603  # Why: static argv built from sys.executable; no shell.
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def test_live_child_exits_promptly_on_sigterm_without_sigkill() -> None:
    """A live child is SIGTERMed and reaped immediately — no 10s
    ``communicate`` stall, no SIGKILL escalation."""
    proc = _spawn([sys.executable, "-c", "import time; time.sleep(60)"])

    start = time.monotonic()
    logs = _shutdown_admin(proc)
    elapsed = time.monotonic() - start

    # -SIGTERM proves the polite signal killed it (SIGKILL would be -SIGKILL).
    assert proc.returncode == -signal.SIGTERM
    assert logs == ""
    # The inverted guard burned the full 10s communicate timeout here; a
    # healthy SIGTERM lands in well under a second. 5s is CI-scheduling
    # headroom, not a measurement.
    assert elapsed < 5.0


def test_already_exited_child_is_drained_without_terminate() -> None:
    """An already-dead child is only drained: buffered output is returned and
    nothing is signaled or waited on."""
    proc = _spawn([sys.executable, "-c", "print('bye')"])
    proc.wait(timeout=10)  # exited before the helper runs

    start = time.monotonic()
    logs = _shutdown_admin(proc)
    elapsed = time.monotonic() - start

    assert logs == "bye"
    assert elapsed < 5.0
