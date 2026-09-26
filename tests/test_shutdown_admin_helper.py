"""Unit tests for the e2e SSE admin-server shutdown helper (container-free).

``_shutdown_admin`` lives at module level in ``tests/e2e/test_progress.py``
(deliberately importable; the e2e lane itself needs Docker). These tests pin
the liveness-guard polarity against real spawned child processes - an
inverted ``poll()`` guard terminates only already-dead children and lets live
ones eat the full ``communicate(timeout=10)`` before SIGKILL, on every
teardown.
"""

import signal
import subprocess
import sys
import time
from pathlib import Path

from tests.e2e.test_progress import _shutdown_admin


def _spawn(args: list[str]) -> subprocess.Popen[str]:
    return subprocess.Popen(  # noqa: S603  # Why: static argv built from sys.executable; no shell.
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_ready(ready_marker: Path, proc: subprocess.Popen[str], cap_secs: float = 60.0) -> None:
    """Wait for the child's readiness marker before the helper runs.

    Why the gate: the measured section must contain the HELPER's ladder on a
    child that is provably past startup, not the child's own cold start. A
    just-forked child spends its first instants in exec and interpreter
    page-in, and under a CI runner's IO storm that page-in runs in
    UNINTERRUPTIBLE IO: a SIGTERM sent there stays pending behind the disk
    wait past the helper's own 10 s escalation budget, the (correct) ladder
    escalates to SIGKILL, and the pin would red on ``-9 == -15`` - run
    36061724490's leg did exactly that, with the helper behaving to the
    letter. The marker is the observable "the child is past startup and
    parked in its sleep": from there a SIGTERM's delivery is the kernel's
    wake-and-die path, milliseconds, and the 5 s bound below is CI
    scheduling headroom, not a measurement.
    """
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"the child exited rc={proc.returncode} before its ready marker")
        if ready_marker.exists():
            return
        time.sleep(0.01)
    proc.kill()
    raise TimeoutError(f"the child's ready marker never appeared within {cap_secs}s")


def test_live_child_exits_promptly_on_sigterm_without_sigkill(tmp_path: Path) -> None:
    """A live child with a graceful SIGTERM handler exits promptly through
    that handler - no 10s ``communicate`` stall, no SIGKILL escalation."""
    # The marker is written by the child itself the instant it is past
    # interpreter startup; the measured section starts only after it. The
    # child installs the graceful handler BEFORE arming the marker, so the
    # SIGTERM's graceful path is armed for the whole measured section (the
    # marker preceding the handler would leave a window where the kernel's
    # default disposition races the install).
    ready_marker = tmp_path / "child_ready"
    proc = _spawn(
        [
            sys.executable,
            "-c",
            f"import pathlib; pathlib.Path({str(ready_marker)!r}).touch(); "
            "import signal, sys; "
            "signal.signal(signal.SIGTERM, lambda *a: sys.exit(0)); "
            "import time; time.sleep(60)",
        ]
    )

    _wait_ready(ready_marker, proc)
    start = time.monotonic()
    logs = _shutdown_admin(proc)
    elapsed = time.monotonic() - start

    # 0 proves the child's own graceful handler handled the SIGTERM (a
    # SIGKILL escalation would be -9; the kernel's default death would be
    # -15). The handler's exit is a handful of bytecodes: the 5s bound is
    # CI-scheduling headroom, not a measurement, and it cannot be starved
    # into the ladder's 10s escalation window because the handler's exit
    # is not IO the runner can starve.
    assert proc.returncode == 0
    assert logs == ""
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
