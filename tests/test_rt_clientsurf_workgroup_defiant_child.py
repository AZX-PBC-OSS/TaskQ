"""Green pin: the supervisor's shutdown escalates to SIGKILL for a child
that ignores SIGTERM.

The workgroup shutdown sequence (src/taskq/worker/workgroup.py:989-1031):
forward SIGTERM to every living child under the restart lock, wait
concurrently for up to ``shutdown_grace``, cancel the stragglers' wait
tasks, then ``proc.kill()`` (SIGKILL) any survivor. Existing pins cover
cooperative children - test_run_forever_graceful_shutdown_via_signal
asserts the SIGTERM was forwarded to a FakeProcess that "exits" on it,
and test_kill_child_timeout_then_sigkill pins the HEALTH-kill helper's
escalation in isolation. No test drives the SHUTDOWN path's escalation
with a child that actually refuses to leave.

This file does: a real child process (short-lived by construction -
killed within ``shutdown_grace`` seconds of the test's signal) traps
SIGTERM and SIGINT and keeps running, marking a file when SIGTERM
arrives so the test can prove the signal reached it. The supervisor
must still complete run_forever (bounded) and reap the child via
SIGKILL - observable as ``returncode == -SIGKILL``.

The spawn is patched to substitute the defiant child for the real
``python -m taskq worker`` command line (the same patch surface the
existing run_forever tests use); everything downstream of the spawn -
signal forwarding, the grace window, the force-kill loop, stream-task
teardown - is the production shutdown path, unpatched. The child is
always reaped by the supervisor's kill; the test's finally block
force-kills and waits again as a belt-and-braces guarantee.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from taskq.worker.workgroup import SupervisorConfig, WorkerSpec, WorkgroupConfig

_DEFIANT_CHILD = """
import signal
import sys
import time
from pathlib import Path

marker = Path(sys.argv[1])

def _recv(signum, frame):
    if signum == signal.SIGTERM:
        marker.write_text("sigterm-received")

signal.signal(signal.SIGTERM, _recv)
signal.signal(signal.SIGINT, _recv)
# Handshake: only after this write are the traps armed - the test waits
# for it so the supervisor's forwarded SIGTERM can never race the child's
# handler installation (an early SIGTERM would kill the child with the
# default disposition and prove nothing about escalation).
marker.write_text("ready")
while True:
    time.sleep(0.2)
"""

_SHUTDOWN_GRACE = 1.5
_SUPERVISOR_BOUND = 12.0
"""Bound on the whole run_forever teardown: shutdown_grace plus generous
margins for stream-task teardown and scheduling - a regression that
hangs shutdown fails the test by name instead of the suite."""


async def test_shutdown_force_kills_a_child_that_ignores_sigterm(tmp_path: Path) -> None:
    """A SIGTERM-defiant child must be reaped by SIGKILL within the grace
    budget, and run_forever must return - the supervisor may never
    outlive a child that refuses to cooperate."""
    marker = tmp_path / "sigterm_marker"
    config = WorkgroupConfig(
        actors="myapp.actors:registry",
        supervisor=SupervisorConfig(shutdown_grace=_SHUTDOWN_GRACE),
        workers=[WorkerSpec(name="defiant", queues=["default"])],
    )
    config_path = Path("/tmp/rt_cs_defiant_workgroup.toml")  # noqa: S108  # Why: same never-read config-path pattern as tests/test_workgroup.py - load_workgroup_config is patched, the path is never opened.

    real_exec = asyncio.create_subprocess_exec
    children: list[asyncio.subprocess.Process] = []
    spawned = asyncio.Event()

    async def defiant_exec(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        proc = await real_exec(
            sys.executable,
            "-c",
            _DEFIANT_CHILD,
            str(marker),
            stdout=kwargs.get("stdout"),
            stderr=kwargs.get("stderr"),
            limit=kwargs.get("limit", 1024),
        )
        children.append(proc)
        spawned.set()
        return proc

    signal_handlers: dict[int, Any] = {}
    sigterm_registered = asyncio.Event()

    def capture_handler(sig: int, handler: Any) -> None:
        signal_handlers[sig] = handler
        if sig == signal.SIGTERM:
            sigterm_registered.set()

    from taskq.worker.workgroup import run_forever

    # Captured BEFORE the loop patch below: inside the with-block
    # asyncio.get_running_loop() is a MagicMock, so all timing reads
    # go through this real reference.
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(run_forever(config_path))
    try:
        with (
            patch("taskq.worker.workgroup.load_workgroup_config", return_value=config),
            patch("asyncio.create_subprocess_exec", side_effect=defiant_exec),
            patch("asyncio.get_running_loop") as mock_loop,
        ):
            mock_loop.return_value.add_signal_handler = capture_handler
            try:
                await asyncio.wait_for(spawned.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("run_forever did not spawn its child within 5.0s")
            try:
                await asyncio.wait_for(sigterm_registered.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("run_forever did not register its SIGTERM handler within 5.0s")

            # Trigger the real shutdown path: the captured handler is the
            # production _on_signal closure installed by run_forever. Wait
            # for the child's ready handshake FIRST so the forwarded SIGTERM
            # is guaranteed to hit armed traps, not the default disposition.
            deadline = loop.time() + 5.0
            while not (marker.exists() and marker.read_text() == "ready"):
                if loop.time() > deadline:
                    pytest.fail("defiant child never armed its signal traps within 5.0s")
                await asyncio.sleep(0.05)
            signal_handlers[signal.SIGTERM]()

            try:
                await asyncio.wait_for(task, timeout=_SUPERVISOR_BOUND)
            except TimeoutError:
                pytest.fail(
                    "CONTRACT: run_forever must complete shutdown within "
                    f"{_SHUTDOWN_GRACE}s grace (bounded to {_SUPERVISOR_BOUND}s "
                    "here) even when a child ignores SIGTERM - the force-kill "
                    "loop (src/taskq/worker/workgroup.py:1014-1023) exists "
                    "precisely so a defiant child cannot wedge the supervisor."
                )

        assert children, "the spawn seam must have recorded the child process"
        proc = children[0]

        # The child is dead and SIGKILL is what reaped it: returncode
        # -SIGKILL can only be set by the kernel after an uncatchable
        # kill - a SIGTERM exit would be 1 (the script would exit on the
        # signal if it did not trap it) or 0.
        deadline = loop.time() + 3.0
        while proc.returncode is None and loop.time() < deadline:  # noqa: ASYNC110  # Why: intentional poll loop - a subprocess's returncode has no asyncio.Event; bounded by the deadline and 50 ms ticks.
            await asyncio.sleep(0.05)
        assert proc.returncode == -signal.SIGKILL, (
            f"the defiant child must be reaped by SIGKILL (returncode "
            f"-{signal.SIGKILL}), got {proc.returncode!r}: the child ignores "
            "SIGTERM by construction, so any other outcome means the "
            "supervisor either never killed it or killed it with the wrong "
            "signal"
        )

        # And the SIGTERM actually reached the child before escalation -
        # the marker is written by the child's own SIGTERM handler.
        assert marker.exists() and marker.read_text() == "sigterm-received", (
            "the supervisor must forward SIGTERM to the child before "
            "escalating to SIGKILL (src/taskq/worker/workgroup.py:989-995); "
            "no marker means the child was killed without ever receiving the "
            "graceful signal"
        )
    finally:
        # Belt-and-braces reap: the supervisor should already have killed
        # and reaped the child; if a regression leaks it, kill and wait
        # here so the test never leaves a live subprocess behind.
        for proc in children:
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(task, timeout=3.0)
