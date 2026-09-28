"""Private test-only worker entry point for the kill9 SIGKILL chaos campaign.

Invoked via ``sys.executable -m tests.system_e2e._kill_entry`` by
``tests/system_e2e/test_kill9_chaos.py``. Loads ``WorkerSettings`` from the
environment, runs the production ``_main`` bootstrap with the FULL system
registry, and honours ONE fault-injection switch:

``TASKQ_KILL9_SEAM`` - which seam the process dies on, every death by
``os.kill(os.getpid(), SIGKILL)`` (no cleanup, no flush drain, no publish:
the OS reaps the process the way an operator's ``kill -9`` does):

``after_claim``
    ``PostgresBackend.dispatch_batch`` dies AFTER the claim statement has
    committed (one autocommit UPDATE...RETURNING) and BEFORE the body can
    start: the row is running, locked, its lease live, no body ran.
``before_terminal``
    ``PostgresBackend.mark_succeeded_with_conn`` / ``mark_succeeded`` die
    BEFORE the terminal write is attempted: the row stays running with a
    live lease while the body's effects are already durable - the
    uncommitted side of the terminal-write window.
``after_terminal``
    The same writes die AFTER they commit (the fused statement has landed
    status + result + attempt row + event) and BEFORE the state-change
    publish / result fanout can run - the committed side of the window,
    and exactly the "between the ledger write and the heartbeat" gap.
``after_heartbeat``
    The worker dies after a beat that actually EXTENDED an in-flight
    job's lease has committed: the row is running with a JUST-extended
    lease (the kill lands inside the visibility window at its widest).

Without the switch the harness is a plain worker on the full registry.
"""

# Why: the schema comes from the worker's own validated TASKQ_SCHEMA_NAME env; the effects insert is $-bound.

from __future__ import annotations

import asyncio
import os
import signal
import sys
from typing import Any

import taskq.worker.heartbeat as _heartbeat_module
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.system_e2e._kill_actors import ACTORS as KILL9_ACTORS
from tests.system_e2e.actors import ACTORS as GRACEFUL_ACTORS

#: The full worker-side registry: the graceful scenarios' actors (the keyed
#: slot scenario kills a holder of ``sys_keyed``'s tenant slot) plus the
#: kill9 campaign's own shapes. The seams below patch BACKEND/module seams,
#: so they land regardless of which actor the worker is executing.
REGISTRY: dict[str, Any] = {**GRACEFUL_ACTORS, **KILL9_ACTORS}

_SEAM = os.environ.get("TASKQ_KILL9_SEAM", "")

_orig_dispatch_batch = PostgresBackend.dispatch_batch
_orig_mark_succeeded = PostgresBackend.mark_succeeded
_orig_mark_succeeded_with_conn = PostgresBackend.mark_succeeded_with_conn
_orig_parse_rowcount = _heartbeat_module.parse_rowcount

_killed = False


def _sigkill() -> None:
    """Die the way a SIGKILL does: no cleanup, no publish, no flush drain."""
    global _killed
    _killed = True
    os.kill(os.getpid(), signal.SIGKILL)


async def _dispatch_batch_then_sigkill(self: Any, *args: Any, **kwargs: Any) -> Any:
    rows = await _orig_dispatch_batch(self, *args, **kwargs)
    # The claim is one autocommit UPDATE...RETURNING: on return it has
    # committed. An empty batch is an idle poll - only a real claim dies.
    if rows and not _killed:
        _sigkill()
    return rows  # pragma: no cover - the process is gone before this returns


async def _mark_succeeded_with_conn_sigkill_first(self: Any, *args: Any, **kwargs: Any) -> Any:
    if not _killed:
        _sigkill()
    return await _orig_mark_succeeded_with_conn(self, *args, **kwargs)  # pragma: no cover


async def _mark_succeeded_with_conn_then_sigkill(self: Any, *args: Any, **kwargs: Any) -> Any:
    row = await _orig_mark_succeeded_with_conn(self, *args, **kwargs)
    if not _killed:
        _sigkill()
    return row  # pragma: no cover


async def _mark_succeeded_then_sigkill(self: Any, *args: Any, **kwargs: Any) -> Any:
    row = await _orig_mark_succeeded(self, *args, **kwargs)
    if not _killed:
        _sigkill()
    return row  # pragma: no cover


def _parse_rowcount_then_sigkill(tag: Any) -> int:
    n = _orig_parse_rowcount(tag)
    # The heartbeat loop reads the renewal tag here on the SUCCESS path,
    # after tx.commit() has returned (worker/heartbeat.py's
    # ``jobs_extended`` debug field). A positive count IS a beat that just
    # extended an in-flight job's lease; an idle beat (0 rows) is not a
    # mid-heartbeat kill, so only a real renewal dies.
    if n > 0 and not _killed:
        _sigkill()
    return n  # pragma: no cover - the process is gone before this returns


if _SEAM == "after_claim":
    PostgresBackend.dispatch_batch = _dispatch_batch_then_sigkill  # type: ignore[assignment]
elif _SEAM == "before_terminal":
    PostgresBackend.mark_succeeded = _mark_succeeded_with_conn_sigkill_first  # type: ignore[assignment]
    PostgresBackend.mark_succeeded_with_conn = (  # type: ignore[assignment]
        _mark_succeeded_with_conn_sigkill_first
    )
elif _SEAM == "after_terminal":
    PostgresBackend.mark_succeeded = _mark_succeeded_then_sigkill  # type: ignore[assignment]
    PostgresBackend.mark_succeeded_with_conn = _mark_succeeded_with_conn_then_sigkill  # type: ignore[assignment]
elif _SEAM == "after_heartbeat":
    # Why the module-global patch: the heartbeat loop does NOT ride
    # ``PostgresBackend.heartbeat_jobs`` - it issues the renewal SQL
    # directly on its pool connection inside the tick's own transaction
    # (and the renewal is THRESHOLD-GATED: only rows whose remaining lease
    # fell under the threshold are re-stamped). The loop reads
    # ``parse_rowcount`` as a module global on each successful tick, so
    # patching the name here intercepts exactly the post-commit renewal
    # observation point.
    _heartbeat_module.parse_rowcount = _parse_rowcount_then_sigkill

if __name__ == "__main__":
    settings = WorkerSettings.load()
    with asyncio.Runner() as runner:
        sys.exit(runner.run(_main(settings, actor_registry=REGISTRY)))
