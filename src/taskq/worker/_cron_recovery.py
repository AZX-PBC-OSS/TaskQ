"""The stale-auto-disable recovery for code-owned cron schedules.

Shared by the two callers that re-declare ownership over the cron table:
the boot-time registration pass (``worker/_bootstrap.py``) and the
leader's takeover pass (``worker/leader.py``). It lives in its own module
because the import direction forbids sharing through either caller:
``_bootstrap`` imports ``leader`` (it constructs ``MaintenanceLeader``),
so the recovery could not live in either without a cycle.

The ownership model (issue #342) and the mixed-version deploy's NULL
marker population (issue #460) are documented on
:func:`revert_stale_auto_disable`; the takeover pass exists because the
boot is not the only moment an old pod's unmarked auto-disable can land
(see :func:`revert_stale_auto_disables`).
"""

from __future__ import annotations

from collections.abc import Sequence

import structlog

from taskq.backend._records import parse_rowcount
from taskq.cron import CronScheduleSpec
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps

__all__ = [
    "revert_stale_auto_disable",
    "revert_stale_auto_disables",
]

_recovery_log = structlog.get_logger("taskq.worker.cron_recovery")


async def revert_stale_auto_disable(
    deps: WorkerDeps,
    settings: WorkerSettings,
    spec: CronScheduleSpec,
) -> bool:
    """Re-enable a schedule row the cron loop auto-disabled, at registration.

    The ownership model (issue #342): ``cron_schedules.disabled_by`` records
    who disabled a row. ``'auto'`` is the cron loop's failure-count
    auto-disable, and the code re-declaring the schedule at startup proves the
    declaration is live again, so the boot reverts the disable (``enabled=true``,
    ``consecutive_failures=0``, ``last_fire_error=NULL``, ``disabled_by=NULL``):
    a transient partial-DB blip (fires fail, strike writes commit) must not
    permanently halt recurring work until a human intervenes. ``'operator'``
    is a deliberate disable (schedule handle, CLI, admin UI, actor
    deregistration) and is NEVER reverted by a boot, exactly the intent the
    create-only registration design guards.

    A disabled row with a NULL marker is read in two populations (issue #460).
    Rows disabled before the column existed were stamped ``'operator'`` by the
    backfill migration (``01.00.19_05``), so they land in the operator case
    above. A residual NULL-disabled row can then only come from an OLD pod
    during a mixed-version rolling deploy: the previous release's failure
    UPDATE writes ``enabled=false`` and cannot name this column. When such a
    row also carries that arm's fingerprint (``consecutive_failures`` at or
    past the auto-disable threshold, ``last_fire_error`` set), it IS an old
    pod's auto-disable -- the deploy's own transient state -- and the recovery
    reverts it like an ``'auto'`` row. A NULL-disabled row without the
    fingerprint reads as an old pod's operator disable during the window and
    stays untouched.

    Only a code-owned, code-enabled spec may revert: an ``owner='operator'``
    spec merely ships the declaration, and a spec declared ``enabled=False``
    does not assert the schedule should run. Returns whether a row was
    re-enabled.
    """
    if spec.owner != "code" or not spec.enabled:
        return False
    async with deps.dispatcher_pool.acquire(timeout=settings.dispatcher_command_timeout) as conn:
        tag: str = await conn.execute(
            f'UPDATE "{settings.schema_name}".cron_schedules '  # noqa: S608  # Why: schema validated against _IDENT_RE at WorkerSettings load; asyncpg cannot bind identifiers, the values below are $-bound.
            f"SET enabled = true, consecutive_failures = 0, last_fire_error = NULL, "
            f"disabled_by = NULL "
            f"WHERE actor = $1 AND name = $2 AND enabled = false AND "
            f"(disabled_by = 'auto' OR (disabled_by IS NULL AND "
            f"consecutive_failures >= $3 AND last_fire_error IS NOT NULL))",
            spec.actor,
            spec.name,
            settings.cron_auto_disable_threshold,
        )
    return parse_rowcount(tag) > 0


async def revert_stale_auto_disables(
    deps: WorkerDeps,
    settings: WorkerSettings,
    specs: Sequence[CronScheduleSpec],
) -> int:
    """Run :func:`revert_stale_auto_disable` over every declared spec.

    The TAKEOVER half of the recovery. The boot pass runs when this pod's
    process starts, but during a mixed-version rolling deploy the OLD
    release can hold (or win) leadership after every new pod has booted:
    its cron tick keeps firing schedules, and its failure arm writes
    ``enabled=false, disabled_by=NULL`` -- an unmarked auto-disable no boot
    pass has since matched, because no boot happens between the old
    leader's strike and the new leader's takeover. Without a takeover
    pass, the schedule the #467 recovery is written for stays disabled
    until the NEXT full restart.

    So every leadership assumption re-runs the recovery over the code's
    declared specs: the new leader inherits the cron table and re-evaluates
    the same ownership predicates the boot applies. The write is idempotent
    and self-guarded (only a code-owned, code-enabled spec reverts; the
    fingerprint conjunct leaves operator intent and pre-ownership rows
    alone), and an old leader cannot fight it: the lease row means at most
    one leader ticks at a time, and a strike that committed in the
    sub-second overlap before the old leader notices its loss is reverted
    by the NEXT assumption (boot or takeover).

    Returns the number of schedules re-enabled. Never raises for a per-spec
    recovery failure: a transient blip reverting one schedule must not
    break the assume path that calls it; the failure is logged and the
    schedule is retried by the next assumption.
    """
    reverted = 0
    for spec in specs:
        try:
            if await revert_stale_auto_disable(deps, settings, spec):
                reverted += 1
                _recovery_log.info(
                    "cron-schedule-auto-disable-reverted-at-takeover",
                    actor=spec.actor,
                    name=spec.name,
                )
        except Exception as exc:  # Why: see the docstring -- a per-spec failure is logged and left to the next assumption; it must not fail the leadership assumption that runs this pass.
            _recovery_log.warning(
                "cron-schedule-takeover-recovery-failed",
                actor=spec.actor,
                name=spec.name,
                error=repr(exc),
                error_type=type(exc).__name__,
            )
    return reverted
