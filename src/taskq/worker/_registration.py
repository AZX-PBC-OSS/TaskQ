"""Shared worker-registration writer: one SQL template, one payload builder.

Two writers stamp a ``workers`` row for the same worker id: the boot
registration (:func:`taskq.worker.run.register_worker`, a strict INSERT
that must raise on a duplicate) and the live re-registration repair -
the heartbeat's zero-row liveness write and the leader election's
FK-violation recovery both restore a row the stale sweep reaped while
the process was alive. The column list and the payload (hostname, pid,
queues, label, instance, and the metadata the dispatch claim's execution
fence reads as data) live here so the two writers cannot drift: a
re-registration that restored a narrower payload than the boot wrote
would silently downgrade the worker's dispatch eligibility.

This module is a leaf: ``run.py`` imports ``_bootstrap.py``, which
imports the heartbeat and the leader, so none of those may import back
through ``run.py`` - all three import this module one-way instead. The
``WORKER_BOOT_WORKFLOW_SCHEMA`` import stays lazy (inside the
capability branch): a worker that never installed ``taskq[flows]`` pays
nothing and never imports ``taskq.workflows`` (the boot-surface import
law, ``tests/test_wf_schema_migration.py``).
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from uuid import UUID

from taskq.backend._protocol import ConnLike
from taskq.backend._records import jsonb_param
from taskq.backend._sql import WORKER_REREGISTRATION_SQL_TEMPLATE
from taskq.constants import require_schema
from taskq.settings import WorkerSettings

__all__ = [
    "reregister_worker",
    "worker_registration_metadata",
    "worker_registration_params",
]


def worker_registration_metadata(
    settings: WorkerSettings, *, workflow_execution: bool = False
) -> dict[str, object]:
    """The workers-row metadata a registration write carries.

    The dispatch claim's EXECUTION fence reads ``workflow_execution`` (and
    ``workflow_execution_schema``) as data, and capacity reads
    ``max_concurrency``, so every registration writer must stamp all of
    them - a repair that restored a row without them would park a capable
    worker's workflow claims fail-closed forever. ``workflow_execution``
    defaults ``False``: a caller that did not run the boot projection is
    not capable, by the same predicate the projection itself answers.
    """
    notify_enabled = getattr(settings, "notify_enabled", False)
    # Schema level 1 is implemented by this build's physical claim trigger and
    # direct-driver refusal; level 2 by the guarded terminal effects. The
    # stamp is what this build actually implements, not an operator override
    # or a claim that future guarded application features are complete.
    # Vanilla workers stamp zero: they still cannot execute workflow bodies
    # under the existing workflow_execution fence, and every pre-existing
    # metadata key survives.
    workflow_execution_schema = 0
    if workflow_execution:
        from taskq.workflows._capability import WORKER_BOOT_WORKFLOW_SCHEMA

        workflow_execution_schema = WORKER_BOOT_WORKFLOW_SCHEMA
    return {
        "notify_enabled": notify_enabled,
        "max_concurrency": settings.max_concurrency,
        "workflow_execution": workflow_execution,
        "workflow_execution_schema": workflow_execution_schema,
    }


def _identity_params(
    settings: WorkerSettings,
) -> tuple[str, int, list[str], str | None, UUID | None]:
    """The five identity parameters ($2..$6 of the template), in order."""
    maybe_instance_raw = settings.workgroup_instance
    return (
        socket.gethostname(),
        os.getpid(),
        settings.queues,
        settings.worker_label,
        UUID(maybe_instance_raw) if maybe_instance_raw else None,
    )


def worker_registration_params(
    settings: WorkerSettings, *, workflow_execution: bool = False
) -> tuple[str, int, list[str], str | None, UUID | None, str | None]:
    """The six bound parameters after the worker id ($2..$7 of the template).

    Returns ``(hostname, pid, queues, worker_label, workgroup_instance,
    metadata_json)`` in the shared template's positional order.
    """
    hostname, pid, queues, label, instance = _identity_params(settings)
    metadata = worker_registration_metadata(settings, workflow_execution=workflow_execution)
    return hostname, pid, queues, label, instance, jsonb_param(metadata)


async def reregister_worker(
    conn: ConnLike,
    worker_id: UUID,
    settings: WorkerSettings,
    *,
    workflow_execution: bool = False,
    tick_metadata: Mapping[str, object] | None = None,
) -> str:
    """Idempotently restore a worker's registration row on a live connection.

    The repair the two liveness sites share: a live, ticking worker whose
    ``workers`` row was reaped executes this against a connection that is
    already open, inside whatever transaction or autocommit boundary the
    caller owns. The statement is ``INSERT ... ON CONFLICT (id) DO
    NOTHING``, so a concurrent repair (the heartbeat tick and the leader
    election can both reach for the same missing row) no-ops against the
    winner's fresh row.

    *tick_metadata* merges the caller's own tick payload under the full
    registration metadata, registration keys taking precedence so nothing
    a tick owns (the stall-attribution tally, ``loop_idle``) can shadow
    the capability keys the dispatch fence reads.

    Returns the command tag: ``INSERT 0 0`` means the row already existed
    (the concurrent-repair case), never an error. The caller decides the
    row's actual fate from its own next read or the statement's failure -
    the tag is not proof either way.
    """
    schema = settings.schema_name
    require_schema(schema)
    metadata = worker_registration_metadata(settings, workflow_execution=workflow_execution)
    if tick_metadata:
        metadata = {**tick_metadata, **metadata}
    sql = WORKER_REREGISTRATION_SQL_TEMPLATE.format(schema=schema)
    return await conn.execute(sql, worker_id, *_identity_params(settings), jsonb_param(metadata))
