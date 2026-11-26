"""Private test-only worker entry point for the kill9 SIGKILL chaos campaign.

Invoked via ``sys.executable -m tests.system_e2e._kill_entry`` by
``tests/system_e2e/test_kill9_chaos.py``. Loads ``WorkerSettings`` from the
environment, runs the production ``_main`` bootstrap with the FULL system
registry, and honours one fault-injection switch:

``TASKQ_KILL9_SEAM`` - which seam the process dies on, every death by
``os.kill(os.getpid(), SIGKILL)`` (no cleanup, no flush drain, no publish:
the OS reaps the process the way an operator's ``kill -9`` does):

``after_claim``
    ``PostgresBackend.dispatch_batch`` dies after the claim statement has
    committed (one autocommit UPDATE...RETURNING) and before the body can
    start: the row is running, locked, its lease live, no body ran.
``before_terminal``
    ``PostgresBackend.mark_succeeded_with_conn`` / ``mark_succeeded`` die
    before the terminal write is attempted: the row stays running with a
    live lease while the body's effects are already durable - the
    uncommitted side of the terminal-write window.
``after_terminal``
    The same writes die after they commit (the fused statement has landed
    status + result + attempt row + event) and before the state-change
    publish / result fanout can run - the committed side of the window,
    and exactly the "between the ledger write and the heartbeat" gap.
``after_heartbeat``
    The worker dies after a beat that actually EXTENDED an in-flight
    job's lease has committed: the row is running with a just-extended
    lease (the kill lands inside the visibility window at its widest).

Without the switch the harness is a plain worker on the full registry.
"""

# Why: the schema comes from the worker's own validated TASKQ_SCHEMA_NAME env; the effects insert is $-bound.

from __future__ import annotations

import os
import signal
import sys
from typing import Any

import taskq.worker.heartbeat as _heartbeat_module
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.worker.run import _main, run_worker_to_exit
from tests.system_e2e._kill_actors import ACTORS as KILL9_ACTORS
from tests.system_e2e.actors import ACTORS as GRACEFUL_ACTORS

#: The full worker-side registry: the graceful scenarios' actors (the keyed
#: slot scenario kills a holder of ``sys_keyed``'s tenant slot) plus the
#: kill9 campaign's own shapes. The seams below patch backend/module seams,
#: so they land regardless of which actor the worker is executing.
REGISTRY: dict[str, Any] = {**GRACEFUL_ACTORS, **KILL9_ACTORS}

_SEAM = os.environ.get("TASKQ_KILL9_SEAM", "")

_orig_dispatch_batch = PostgresBackend.dispatch_batch
_orig_mark_succeeded = PostgresBackend.mark_succeeded
_orig_mark_succeeded_with_conn = PostgresBackend.mark_succeeded_with_conn
_orig_heartbeat_logger = _heartbeat_module.logger

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


class _AfterHeartbeatLoggerProxy:
    """Delegating proxy over ``heartbeat.logger`` that SIGKILLs the worker
    immediately after a committed beat that actually extended at least one
    in-flight job's lease.

    Keying on the post-commit ``heartbeat-tick-success`` event's explicit
    ``jobs_extended`` field (rather than patching ``parse_rowcount``) keeps
    the seam immune to other ``parse_rowcount`` calls inside the tick
    transaction (such as the worker-liveness row check).
    """

    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped

    def debug(self, event: Any = None, *args: Any, **kwargs: Any) -> Any:
        jobs_extended = kwargs.get("jobs_extended", 0)
        if (
            event == "heartbeat-tick-success"
            and type(jobs_extended) is int
            and jobs_extended > 0
            and not _killed
        ):
            _sigkill()
        return self._wrapped.debug(event, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)


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
    # Why the module-logger proxy: the heartbeat loop issues the renewal SQL
    # directly on its pool connection inside the tick transaction, and emits
    # ``logger.debug("heartbeat-tick-success", ..., jobs_extended=...)``
    # immediately after tx.commit() returns. Intercepting that post-commit
    # event ensures only a committed lease renewal (jobs_extended > 0) dies,
    # never an idle boot beat or an in-transaction liveness row check.
    _heartbeat_module.logger = _AfterHeartbeatLoggerProxy(_orig_heartbeat_logger)  # type: ignore[assignment]  # Why: _AfterHeartbeatLoggerProxy dynamically delegates BoundLogger via __getattr__ for the after_heartbeat fault-injection seam.

if __name__ == "__main__":
    settings = WorkerSettings.load()
    sys.exit(run_worker_to_exit(_main(settings, actor_registry=REGISTRY)))
