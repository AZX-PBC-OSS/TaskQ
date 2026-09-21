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
  retry via ``retry_job``, schedule run-now via ``enqueue``, rate-limit
  reset via the registry): the backend commits its own transaction, so the
  audit row is written on a separate checkout immediately after the backend
  call reports success, and a failure to record is logged loudly
  (``admin-audit-record-failed``) AND counted on the
  ``taskq.admin.audit.record_failed`` metric -- alert on a nonzero rate,
  that is the only signal a silently-degrading trail gives -- rather than
  failing a mutation that already landed. For cancel,
  :func:`fold_principal_into_cancel_event`
  additionally folds the principal into the new ``cancel_request``
  job_events row's detail, so the per-job event log carries the operator
  identity too.

Importing this module requires nothing beyond the core package; the
``taskq[fastapi]`` extra is only needed by the routes that call it.
"""

from typing import Any

import structlog
from opentelemetry.metrics import Counter

from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import ConnLike
from taskq.obs import get_meter
from taskq.web._pool import BoundedPool

logger = structlog.get_logger("taskq.web.admin.audit")

# Alertability for the degradation window: every failed audit record on the
# backend-mediated paths increments this counter, so the degrade-to-warn
# behavior is a METRIC (``taskq.admin.audit.record_failed``), not just a log
# line. A log line is not alertable, and the window it reports can persist
# indefinitely (a wedged pool, an unmigrated schema), so the compensating
# control for warn-mode is an alert on a nonzero rate of this counter.
# Doc: docs/guides/admin-ui.md, "When the audit record itself fails".
_record_failed_counter: Counter = get_meter().create_counter(
    name="taskq.admin.audit.record_failed",
    description=(
        "Admin audit rows that could not be recorded after a backend-mediated "
        "mutation already committed (warn-mode degradation)."
    ),
    unit="1",
)

# The principal recorded when the router runs with ``auth_dependency=None``
# (dev deployments only; the factory fails closed everywhere else). The
# value is explicit rather than NULL so a reader never confuses "no auth
# configured" with a row someone failed to attribute.
ANONYMOUS_SUBJECT: str = "anonymous"

# The principal_subject column's shape bound. The router accepts ANY auth
# dependency a host passes, and its return value is untyped
# (:func:`principal_subject` normalizes whatever arrives), so the subject's
# shape is pinned here rather than trusted: an unbounded string (a bug, or
# a hostile claims provider) would write an unbounded row into the trail
# and the folded event detail, and a control character would survive into
# rendered pages and line-oriented log readers. 512 matches the cancel
# form's ``maxlength`` on its reason input -- the other free-text field
# this module writes.
SUBJECT_MAX_LENGTH: int = 512

# Control-character escapes for the subject: the same log-frame discipline
# ops.py applies to caller-controlled log fields, applied at the audit
# boundary. \n/\r/\t keep their letter escapes for readability; every other
# C0 control and DEL renders as its hex escape. NUL is included (0x00):
# asyncpg refuses \u0000 in a text bind, so an unescaped NUL would make
# every same-transaction audit insert (and therefore the mutation itself)
# fail, and every safe-path record degrade.
_SUBJECT_CONTROL_ESCAPES: dict[int, str] = {
    **{c: f"\\x{c:02x}" for c in range(0x20)},
    0x7F: "\\x7f",
    ord("\n"): "\\n",
    ord("\r"): "\\r",
    ord("\t"): "\\t",
}


def _bound_subject(subject: str) -> str:
    """Pin a resolved subject to the audit column's shape: no control
    characters, bounded length. Truncation is the honest bound -- the row
    records what arrived, capped; the alternative (rejecting) would let a
    broken auth dependency take down every admin mutation."""
    escaped = subject.translate(_SUBJECT_CONTROL_ESCAPES)
    return escaped[:SUBJECT_MAX_LENGTH]


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
ACTION_RATE_LIMIT_RESET: str = "rate_limit.reset"

TARGET_TYPE_JOB: str = "job"
TARGET_TYPE_SCHEDULE: str = "schedule"
TARGET_TYPE_ACTOR: str = "actor"
TARGET_TYPE_RATE_LIMIT_BUCKET: str = "rate_limit_bucket"

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

    The resolved string is shape-pinned (:func:`_bound_subject`): control
    characters escaped, length capped at :data:`SUBJECT_MAX_LENGTH`. The
    router cannot trust the dependency's return shape, and the subject
    reaches a text bind, a jsonb detail document, and rendered pages.
    """
    if principal is None:
        return ANONYMOUS_SUBJECT
    subject = getattr(principal, "subject", None)
    if isinstance(subject, str) and subject:
        return _bound_subject(subject)
    if isinstance(principal, str) and principal:
        return _bound_subject(principal)
    return _bound_subject(str(principal))


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
    failed because its bookkeeping row could not be written (the state
    change is done; a fake-failure response would invite a retry of an
    already-applied action), so any failure here (``admin_audit`` not
    migrated yet, pool wedged) is logged as
    ``admin-audit-record-failed`` AND counted on the
    ``taskq.admin.audit.record_failed`` OTel counter, and swallowed.
    Warn-mode is deliberate rather than an oversight: post-commit there is
    no fail-closed option that tells the truth -- the only alternative is
    lying about a landed mutation -- so the compensating control is the
    counter. ALERT ON IT: any nonzero rate means mutations are landing
    un-attributed right now. The admin-owned-SQL paths do NOT get this
    wrapper -- there the audit row is in the mutation's own transaction
    and must fail it (fail-closed, where fail-closed is possible).
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
        _record_failed_counter.add(1, {"action": action, "operation": "record"})
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
    (:data:`logger` and the record-failed counter): the audit row and the
    cancel itself have already landed.

    Known narrow window: the fold re-finds "the" event by (job_id, kind,
    id DESC) rather than by the id ``write_cancel_request`` wrote -- the
    backend protocol returns only a bool. A NON-admin cancel_request
    writer (a client calling the backend directly) committing inside that
    window would receive the fold meant for the operator's event. The
    admin_audit row -- always written first, target-keyed -- remains the
    authoritative attribution; the folded detail is the convenience copy.
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                _FOLD_CANCEL_PRINCIPAL_SQL.format(schema=schema),
                job_id,
                principal_subject(principal),
            )
    except Exception as exc:
        _record_failed_counter.add(1, {"action": ACTION_JOB_CANCEL, "operation": "fold"})
        logger.warning(
            "admin-audit-cancel-fold-failed",
            target_type=TARGET_TYPE_JOB,
            target_id=str(job_id),
            error_type=type(exc).__name__,
            error=str(exc),
        )
