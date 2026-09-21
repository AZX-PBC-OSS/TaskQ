"""Admin audit trail: durable record of who did what through the admin UI.

Every admin-UI operator mutation inserts one ``admin_audit`` row through
this module. The table (migration ``01.00.19_01_pre_admin_audit``) carries
no foreign key to jobs on purpose: its targets are exactly the rows routine
maintenance prunes, archives, and deregisters, and an audit trail that dies
with its target is not a trail.

Two transactional shapes, per mutation kind:

* Admin-owned SQL (schedule enable/disable/skip, actor deregister): the
  caller opens the transaction and calls :func:`record_admin_action` on the
  SAME connection inside it, so the mutation and its audit row commit or
  roll back together -- a mutation that lands without its audit row is the
  defect this module exists to prevent.
* Backend-mediated mutations (job cancel via ``write_cancel_request``, job
  retry via ``retry_job``): the backend commits its own transaction, so the
  audit row is written on a separate checkout immediately after the backend
  call reports success, and a failure to record is logged loudly
  (``admin-audit-record-failed``) rather than failing a mutation that
  already landed. For cancel, :func:`fold_principal_into_cancel_event`
  additionally folds the principal into the new ``cancel_request``
  job_events row's detail, so the per-job event log carries the operator
  identity too.

Importing this module requires nothing beyond the core package; the
``taskq[fastapi]`` extra is only needed by the routes that call it.
"""

from typing import Any

import structlog

from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import ConnLike
from taskq.web._pool import BoundedPool

logger = structlog.get_logger("taskq.web.admin.audit")

# The principal recorded when the router runs with ``auth_dependency=None``
# (dev deployments only; the factory fails closed everywhere else). The
# value is explicit rather than NULL so a reader never confuses "no auth
# configured" with a row someone failed to attribute.
ANONYMOUS_SUBJECT: str = "anonymous"

# ── Closed-set action names ──────────────────────────────────────────────
#
# Kept as module constants (not an enum) so the routes stay plain-Python
# and the SQL COMMENT on admin_audit.action documents the same set by hand;
# the unit tests pin the routes to exactly these strings.

ACTION_JOB_CANCEL: str = "job.cancel"
ACTION_JOB_RETRY: str = "job.retry"
ACTION_SCHEDULE_ENABLE: str = "schedule.enable"
ACTION_SCHEDULE_DISABLE: str = "schedule.disable"
ACTION_SCHEDULE_SKIP: str = "schedule.skip"
ACTION_SCHEDULE_RUN: str = "schedule.run"
ACTION_ACTOR_DEREGISTER: str = "actor.deregister"

TARGET_TYPE_JOB: str = "job"
TARGET_TYPE_SCHEDULE: str = "schedule"
TARGET_TYPE_ACTOR: str = "actor"

_INSERT_SQL = (
    'INSERT INTO "{schema}".admin_audit '
    "(principal_subject, action, target_type, target_id, reason, detail) "
    "VALUES ($1, $2, $3, $4, $5, $6::jsonb)"
)

# The cancel handler folds the operator principal into the NEWEST
# cancel_request event of the job (id DESC: bigserial carries the stream
# order). Escaped braces on the jsonb empty-object literal: this string
# goes through str.format.
_FOLD_CANCEL_PRINCIPAL_SQL = (
    'UPDATE "{schema}".job_events '
    "SET detail = COALESCE(detail, '{{}}'::jsonb) "
    "|| jsonb_build_object('principal_subject', $2::text) "
    'WHERE id = (SELECT id FROM "{schema}".job_events '
    "WHERE job_id = $1 AND kind = 'cancel_request' "
    "ORDER BY id DESC LIMIT 1)"
)


def principal_subject(principal: Any) -> str:
    """Normalize whatever the auth dependency returned to an audit subject.

    The router stores the auth dependency's return value on
    ``request.state.principal`` untyped: the shipped SSO dependencies
    return :class:`IdentityClaims` (read ``.subject``), but a host
    application may pass any dependency, and the dev path (no
    ``auth_dependency``) has ``None``. The audit column is ``NOT NULL``,
    so every shape resolves to a string, with the explicit
    :data:`ANONYMOUS_SUBJECT` for the no-auth case rather than a crash or
    an empty string.
    """
    if principal is None:
        return ANONYMOUS_SUBJECT
    subject = getattr(principal, "subject", None)
    if isinstance(subject, str) and subject:
        return subject
    if isinstance(principal, str) and principal:
        return principal
    return str(principal)


async def record_admin_action(
    conn: ConnLike,
    *,
    schema: str,
    principal: Any,
    action: str,
    target_type: str,
    target_id: str,
    reason: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """Insert one ``admin_audit`` row on the caller's connection.

    The caller owns the transaction: on the admin-owned-SQL paths the call
    must sit inside the same ``conn.transaction()`` block as the mutation
    (that is the same-transaction guarantee), and on the backend-mediated
    paths it runs on its own checkout after the backend reports success.
    *detail* is bound as jsonb (:func:`taskq._json.dumps_jsonb_str`, the
    NUL-refusing serializer every other jsonb bind uses).
    """
    await conn.execute(
        _INSERT_SQL.format(schema=schema),
        principal_subject(principal),
        action,
        target_type,
        target_id,
        reason,
        dumps_jsonb_str(detail if detail is not None else {}),
    )


async def record_admin_action_safe(
    pool: BoundedPool,
    *,
    schema: str,
    principal: Any,
    action: str,
    target_type: str,
    target_id: str,
    reason: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """Record the audit row on its own bounded checkout, degrading loudly.

    For the backend-mediated mutations, whose backend call has already
    committed by the time this runs: the mutation must not be reported as
    failed because its bookkeeping row could not be written, so any
    failure here (``admin_audit`` not migrated yet, pool wedged) is
    logged as ``admin-audit-record-failed`` and swallowed. The
    admin-owned-SQL paths do NOT get this wrapper -- there the audit row
    is in the mutation's own transaction and must fail it.
    """
    try:
        async with pool.acquire() as conn:
            await record_admin_action(
                conn,
                schema=schema,
                principal=principal,
                action=action,
                target_type=target_type,
                target_id=target_id,
                reason=reason,
                detail=detail,
            )
    except Exception as exc:
        logger.warning(
            "admin-audit-record-failed",
            action=action,
            target_type=target_type,
            target_id=target_id,
            error_type=type(exc).__name__,
            error=str(exc),
        )


async def fold_principal_into_cancel_event(
    pool: BoundedPool,
    *,
    schema: str,
    job_id: Any,
    principal: Any,
) -> None:
    """Fold the operator principal into the newest cancel_request event.

    Called right after a successful ``backend.write_cancel_request`` (and
    after :func:`record_admin_action_safe`): the per-job event log
    (``job_events``) is what an operator reads page-by-page, so the
    cancel_request row there carries ``principal_subject`` in its detail
    jsonb next to the reason, instead of the identity living only in the
    admin_audit table. The newest event is the one this very cancel just
    wrote; the handler only calls this when ``write_cancel_request``
    returned True (a False wrote no event, and re-writing an OLDER cancel
    event's identity would be a falsification). Failures degrade loudly
    (:data:`logger`): the audit row and the cancel itself have already
    landed.
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                _FOLD_CANCEL_PRINCIPAL_SQL.format(schema=schema),
                job_id,
                principal_subject(principal),
            )
    except Exception as exc:
        logger.warning(
            "admin-audit-cancel-fold-failed",
            target_type=TARGET_TYPE_JOB,
            target_id=str(job_id),
            error_type=type(exc).__name__,
            error=str(exc),
        )
