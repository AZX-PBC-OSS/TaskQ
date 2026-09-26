"""Unit tests for the e2e SSE admin-server shutdown helper (container-free).

``_shutdown_admin`` lives at module level in ``tests/e2e/test_progress.py``
(deliberately importable; the e2e lane itself needs Docker). These tests pin
the liveness-guard polarity against real spawned child processes - an
inverted ``poll()`` guard terminates only already-dead children and lets live
ones eat the full ``communicate(timeout=10)`` before SIGKILL, on every
teardown.

Why the earlier CI legs saw ``-9 == -15`` here: the xdist worker that ran an
in-process ``worker_main`` had been left with ``SIGTERM = SIG_IGN`` by that
call's trailing guard; every subprocess it spawned afterwards inherited the
ignored disposition (exec preserves "ignored", unlike handlers), the helper's
SIGTERM was discarded, the 10s escalation budget expired, and the correct
ladder SIGKILLed the child. Proven by reproduction: the child itself reported
``SIG_IGN`` and died ``rc == -9`` at exactly 10.0s, cold and warm alike. The
guard now lives at the entrypoints whose next act is their own death, the
worker's baseline dispositions are restored after every test
(``_host_signal_dispositions_restored`` in ``tests/conftest.py``), and the
leak itself is pinned in ``test_worker_main_leaves_the_host_signal_disposition_alone``.
The ready-marker gate below still earns its keep: it bounds the measured
section to a child provably past startup and parked in its sleep, so the
ladder's timing measures the helper, not the child's cold start.
"""

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

    Why the gate: the measured section must contain the HELPER's ladder on
    a child that is provably past startup and parked in its sleep - from
    there a SIGTERM's delivery is the kernel's wake-and-die path,
    milliseconds, and the 5s bound below is CI-scheduling headroom, not a
    measurement. Without the gate the ladder could measure the child's own
    cold start (exec and interpreter page-in) instead of the helper. The
    historical ``-9 == -15`` CI legs are explained (and pinned) by the
    inherited-``SIG_IGN`` leak documented in the module docstring - not by
    startup timing - so this gate is a measurement-hygiene boundary, not
    the fix.
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
    # The child installs the graceful handler FIRST and touches the ready
    # marker SECOND: the marker is the observable "past startup, handler
    # armed, parked in its sleep", so the measured section contains no
    # window where the SIGTERM could land on the kernel's default
    # disposition (or on an inherited SIG_IGN - see the disposition-leak
    # pin in test_worker_main.py - which would DISCARD the signal and
    # force the helper's SIGKILL escalation).
    ready_marker = tmp_path / "child_ready"
    proc = _spawn(
        [
            sys.executable,
            "-c",
            "import pathlib, signal, sys; "
            "signal.signal(signal.SIGTERM, lambda *a: sys.exit(0)); "
            f"pathlib.Path({str(ready_marker)!r}).touch(); "
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


def test_live_default_disposition_child_dies_to_sigterm(tmp_path: Path) -> None:
    """A live child with NO handler dies by the kernel's default SIGTERM
    death - promptly, without the helper's SIGKILL escalation."""
    # No handler is installed: the child parks in ``time.sleep`` with the
    # disposition it was spawned with (SIG_DFL - the worker's own baseline,
    # held there by the disposition-hygiene fixture in tests/conftest.py).
    ready_marker = tmp_path / "child_ready"
    proc = _spawn(
        [
            sys.executable,
            "-c",
            f"import pathlib; pathlib.Path({str(ready_marker)!r}).touch(); "
            "import time; time.sleep(60)",
        ]
    )

    _wait_ready(ready_marker, proc)
    start = time.monotonic()
    logs = _shutdown_admin(proc)
    elapsed = time.monotonic() - start

    # -SIGTERM proves the polite signal killed it (SIGKILL would be -9;
    # a handler's clean exit would be 0).
    assert proc.returncode == -15
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
