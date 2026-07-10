"""Async file-watch + subprocess-restart loop for ``taskq dev``.

Spawns the worker as a child subprocess via ``taskq worker --actors <module:attr>``
and restarts it on file-change events from ``watchfiles.awatch()``. Each restart
gets a fresh Python interpreter with clean import state — no ``importlib.reload()``
required.

Layering: imports only stdlib, ``watchfiles`` (optional, imported inside function
body), and ``taskq.obs`` (structlog). Does NOT import from ``taskq.worker.run``,
``taskq.worker.deps``, or any backend.
"""

import asyncio
import contextlib
import importlib
import os
import shutil
import signal
import sys
from collections.abc import Sequence
from pathlib import Path

from taskq.obs import get_logger

__all__ = ["dev_watch_loop"]


def _validate_import(module_attr: str) -> bool:
    """Check that *module_attr* (``"pkg.mod:attr"``) is importable.

    Prints a human-readable error line to stderr and returns ``False`` on any
    failure. Returns ``True`` on success. Never raises.
    """
    module_name, sep, attr_name = module_attr.partition(":")
    if not sep or not module_name or not attr_name:
        print(
            f"expected module:attr syntax (e.g. myapp.actors:registry); got {module_attr!r}",
            file=sys.stderr,
        )
        return False

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        print(f"cannot import '{module_name}': {type(exc).__name__}: {exc}", file=sys.stderr)
        return False

    try:
        getattr(module, attr_name)
    except AttributeError:
        print(f"attribute {attr_name!r} not found in module {module_name}", file=sys.stderr)
        return False

    return True


async def _start_worker(module_attr: str) -> asyncio.subprocess.Process:
    """Spawn a worker subprocess and return the process handle.

    The child inherits the parent's full environment. Child stdout/stderr flow
    directly to the terminal — no PIPE (avoids deadlock when pipe buffer fills).
    Uses the ``taskq`` console script so that the worker is invoked identically
    to a developer typing ``taskq worker --actors <module:attr>`` at a shell.
    """
    taskq_exe = shutil.which("taskq")
    if taskq_exe is None:
        msg = (
            "Could not find the 'taskq' executable on PATH. "
            "Ensure taskq is installed (e.g. with uv tool install taskq)."
        )
        raise RuntimeError(msg)
    proc = await asyncio.create_subprocess_exec(
        taskq_exe,
        "worker",
        "--actors",
        module_attr,
    )
    return proc


async def _stop_worker(proc: asyncio.subprocess.Process, grace_period: float) -> None:
    """Send SIGTERM, wait *grace_period* seconds, then SIGKILL if still alive.

    A *grace_period* of 0 means: send SIGTERM, yield control once, then kill
    if still alive.
    """
    try:
        proc.send_signal(signal.SIGTERM)
    except ProcessLookupError:
        await proc.wait()
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=grace_period)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()


async def _monitor_worker_exit(proc: asyncio.subprocess.Process) -> None:
    """Background task: await ``proc.wait()`` and warn if the worker exits unexpectedly."""
    returncode = await proc.wait()
    if returncode != 0:
        log = get_logger("taskq.worker.dev")
        log.warning("dev-worker-exit", returncode=returncode)


async def dev_watch_loop(
    module_attr: str,
    *,
    watch_paths: Sequence[str | Path],
    grace_period: float,
) -> None:
    """Async file-watch + subprocess-restart loop. Never returns normally;
    exits on SIGINT or unrecoverable error."""
    try:
        from watchfiles import (
            DefaultFilter,
            awatch,  # pyright: ignore[reportUnknownVariableType]  # Why: watchfiles ships no pyright-compatible stubs; awatch return type is partially unknown.
        )
    except ImportError:
        print(
            "watchfiles is required for 'taskq dev'. Install it with:\n"
            '  pip install "taskq-py[reload]"',
            file=sys.stderr,
        )
        raise SystemExit(1) from None

    log = get_logger("taskq.worker.dev")

    proc = await _start_worker(module_attr)
    monitor_task = asyncio.create_task(_monitor_worker_exit(proc))

    try:
        async for changes in awatch(*watch_paths, watch_filter=DefaultFilter(), debounce=400):
            changed_files = [
                os.path.relpath(c[1], Path.cwd())  # noqa: ASYNC240 Why: os.path.relpath on already-resolved paths is pure string arithmetic (no I/O, no stat calls); trio/anyio are not project dependencies.
                for c in changes
            ]
            log.info(
                "dev_reload_triggered", kind="dev_reload_triggered", changed_files=changed_files
            )

            await _stop_worker(proc, grace_period)
            monitor_task.cancel()
            with contextlib.suppress(TimeoutError, asyncio.CancelledError):
                await monitor_task

            if not _validate_import(module_attr):
                log.warning("dev-import-failed", module_attr=module_attr)
                continue

            proc = await _start_worker(module_attr)
            monitor_task = asyncio.create_task(_monitor_worker_exit(proc))
    except (KeyboardInterrupt, asyncio.CancelledError):
        await _stop_worker(proc, grace_period)
        monitor_task.cancel()
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            await monitor_task
        raise SystemExit(0) from None
