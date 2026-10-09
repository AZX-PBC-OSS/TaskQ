"""The admin audit trail's WRITER — the deps-free canonical home.

Every operator mutation (the admin UI's, the CLI's, and the workflow
engine's cancel/retry verbs) inserts one ``admin_audit`` row through this
module. The table (migration ``01.00.19_03_pre_admin_audit``) carries no
foreign key to jobs on purpose: its targets are exactly the rows routine
maintenance prunes, archives, and deregisters, and an audit trail that
dies with its target is not a trail.

THE IMPORT LAW (§16.1; the attack pins' Face C —
``tests/test_wf_attack_cancel.py``): this module is CORE-DEPS-ONLY —
structlog, opentelemetry-api (a core dependency), and the taskq core;
never the ``fastapi`` extra. The workflow engine's cancel/retry seams
(the G4 law: "who cancelled this" is a ROW, not a log line) import FROM
HERE and from nowhere else: ``taskq/workflows/`` must never import
``taskq.web.admin`` (whose package ``__init__`` imports the fastapi
extra — a base ``taskq[flows]`` install would be unable to cancel).
The admin surface's ``taskq.web.admin._audit`` is a COMPAT SHIM over
this module (its routes and tests keep their import path); the web
package's fastapi-bearing modules never load on the engine's path.

Two transactional shapes, per mutation kind:

* Admin-owned SQL (schedule enable/disable/skip, actor deregister, the
  workflow engine's cancel/retry): the caller opens the transaction and
  calls :func:`record_admin_action` on the SAME connection inside it, so
  the mutation and its audit row commit or roll back together -- a
  mutation that lands without its audit row is the defect this module
  exists to prevent.
* Backend-mediated mutations (job cancel via ``write_cancel_request``,
  job retry via ``retry_job``, schedule run-now via ``enqueue``,
  rate-limit reset via the registry): the backend commits its own
  transaction, so the audit row is written on a separate checkout
  immediately after the backend call reports success, and a failure to
  record is logged loudly (``admin-audit-record-failed``) AND counted on
  the ``taskq.admin.audit.record_failed`` metric -- alert on a nonzero
  rate, that is the only signal a silently-degrading trail gives --
  rather than failing a mutation that already landed. For cancel,
  :func:`fold_principal_into_cancel_event` additionally folds the
  principal into the new ``cancel_request`` job_events row's detail, so
  the per-job event log carries the operator identity too.

THE CAPABILITY SEAM (the one indirection, and why it exists): the
engine's audit row and the admin surface's audit row are THE SAME row —
one writer, one attribute surface. When the admin shim is ALREADY LOADED
(a ``taskq[fastapi]`` install — its presence in ``sys.modules`` is the
load-state registry's marker; this module NEVER imports it), the row
resolves through the shim's CURRENT module attribute, so the admin
surface's attribute seam stays the one seam — the test doubles (and any
host's instrumentation) that rebind the shim's
``record_admin_action`` bite every writer in the process, including the
engine's. When the shim is not loaded (the base install) this module's
own body is the writer. The resolution is a dictionary read, never an
import: a base install never touches the fastapi extra.

THE SEAM'S TWO DEFENSES (both born red — the red-team's CHANGES-
REQUIRED; the teeth pins in ``tests/test_wf_attack_cancel.py`` convict
each live): the FROZEN canonical identity (``_CANONICAL_RECORD`` — a
module-attr rebind of this module's own ``record_admin_action`` cannot
make the body route back to itself) and the RE-ENTRANCY guard (the
``_routing`` contextvar — a delegating chain through the shim's attr
terminates with EXACTLY ONE row per call chain). The MID-CALL WRITER-
SWITCH boundary: the writer is resolved once at the seam's entry; a
rebinding between the resolution and the write is observed by the NEXT
call chain, never this one's row.
"""

from __future__ import annotations

import contextvars
import sys
from typing import TYPE_CHECKING, Any

import structlog
from opentelemetry.metrics import Counter

from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import ConnLike
from taskq.obs import get_meter
from taskq.obs._redact_exc import mask_credentials

if TYPE_CHECKING:
    # The pool type is the web package's bounded pool — an ANNOTATION
    # only (the import law: the runtime path here is deps-free; the web
    # package's modules import the fastapi extra).
    from taskq.web._pool import BoundedPool

__all__ = [
    "ACTION_ACTOR_DEREGISTER",
    "ACTION_JOB_CANCEL",
    "ACTION_JOB_RETRY",
    "ACTION_RATE_LIMIT_RESET",
    "ACTION_SCHEDULE_DISABLE",
    "ACTION_SCHEDULE_ENABLE",
    "ACTION_SCHEDULE_RUN",
    "ACTION_SCHEDULE_SKIP",
    "ANONYMOUS_SUBJECT",
    "SUBJECT_MAX_LENGTH",
    "TARGET_TYPE_ACTOR",
    "TARGET_TYPE_JOB",
    "TARGET_TYPE_RATE_LIMIT_BUCKET",
    "TARGET_TYPE_SCHEDULE",
    "fold_principal_into_cancel_event",
    "principal_subject",
    "record_admin_action",
    "record_admin_action_safe",
]

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


def _bound_untyped_subject(raw: str) -> str:
    """Redact-or-bind for the UNTYPED principal shapes (#463).

    A bare string principal is whatever the host's auth dependency put on
    ``request.state.principal`` -- and the admin-ui guide's own worked
    example returns ``credentials.credentials``, so the string is sometimes
    the operator's raw bearer credential. That must never reach the ledger:
    the ``admin_audit`` table is deliberately never pruned, and the
    subject is rendered on the job detail page -- a stored token there is a
    durable, page-rendered credential disclosure. The house credential
    mask chain (:func:`taskq.obs.mask_credentials`) runs BEFORE the shape
    pin, so the token material leaves as ``***`` while any non-secret frame
    (``Bearer ``) survives and the row still attributes the action to a
    redacted marker. The mask fires loudly: a credential-shaped principal
    means the auth dependency is leaking, and the operator must learn it
    from the log (the log line carries no token material itself).

    The typed path (:func:`principal_subject` reading a ``.subject``
    attribute) deliberately skips the MASK -- a claims object's ``subject``
    is by contract the identity field, and over-redacting identity is its
    own falsification -- but not the DETECTION: the same mask chain runs
    on the subject and a credential-shaped one is logged with the same
    loud event, so the operator learns of the leak either way (#463). A
    HOSTILE ``.subject`` is still bounded and control-escaped by
    :func:`_bound_subject`.
    """
    masked = mask_credentials(raw)
    if masked != raw:
        logger.warning(
            "admin-audit-principal-credential-redacted",
            detail=(
                "the auth dependency returned a bare credential-shaped string "
                "(a bearer/JWT-shaped token); its secret material was masked "
                "before the audit bind. Fix the dependency to return a subject, "
                "not the credential."
            ),
        )
    return _bound_subject(masked)


def _warn_typed_subject_credential_shaped(subject: str) -> None:
    """The typed-path detection half of :func:`_bound_untyped_subject`.

    Same mask chain, same loud event -- but the bind stays verbatim (the
    subject field IS the identity; masking it is the over-redaction the
    typed path exists to avoid). The detail tells that truth: the row
    carries what arrived, and the operator must fix the dependency.
    """
    if mask_credentials(subject) != subject:
        logger.warning(
            "admin-audit-principal-credential-redacted",
            detail=(
                "the auth dependency returned a claims principal whose "
                ".subject is credential-shaped (bearer/JWT-shaped token "
                "material); the subject field is bound verbatim by the "
                "identity contract. Fix the dependency to return a subject, "
                "not the credential."
            ),
        )


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

#: The loaded admin surface's audit module — the CAPABILITY SEAM's read
#: (the load-state registry: the shim's PRESENCE is the fastapi install's
#: capability marker). NEVER an import: on a base install this answers
#: None and this module's own body is the writer.
_ADMIN_AUDIT_SHIM = "taskq.web.admin._audit"

#: THE RE-ENTRANCY GUARD (the red-team's 1c cure): True while THIS call
#: chain is routing through the loaded shim's writer. A CONTEXTVAR —
#: the re-entrancy domain is the async call chain: asyncio copies the
#: context per Task (and per thread), so two concurrent cancels never
#: cross-see the flag (a threading.local would leak it across tasks
#: interleaving on one loop; a sentinel attr on the wrapper fails on
#: attribute-less callables and leaks the same way). The flag dies with
#: the chain (the token reset) — no cleanup, no cross-talk.
_routing: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "taskq.audit.seam-routing", default=False
)


def _loaded_admin_audit() -> Any:
    """The already-loaded admin shim's module object, or ``None`` — never
    an import (the capability seam's one rule)."""
    return sys.modules.get(_ADMIN_AUDIT_SHIM)


def _failed_counter() -> Counter:
    """The degradation counter through the capability seam: the loaded
    shim's ``_record_failed_counter`` attribute when the shim is loaded
    (the admin redteams' patch surface stays the ONE seam), this
    module's own otherwise."""
    mod = _loaded_admin_audit()
    if mod is not None:
        counter = getattr(mod, "_record_failed_counter", None)
        if counter is not None:
            return counter
    return _record_failed_counter


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

    The UNTYPED shapes (a bare string principal, and the ``str()``
    fallback) additionally run the house credential mask chain BEFORE the
    shape pin (:func:`_bound_untyped_subject`, #463): a bare credential-
    shaped string -- the shape the admin-ui guide's own worked example
    produces -- is redacted to the ``***`` marker and logged loudly
    (``admin-audit-principal-credential-redacted``) instead of storing the
    operator's bearer token verbatim in a table that is never pruned and
    is rendered on the job detail page. A typed subject-bearing principal
    binds its ``subject`` field -- verbatim (the identity contract), but
    with the same loud detection log when the field is credential-shaped
    (:func:`_warn_typed_subject_credential_shaped`).
    """
    if principal is None:
        return ANONYMOUS_SUBJECT
    subject = getattr(principal, "subject", None)
    if isinstance(subject, str) and subject:
        _warn_typed_subject_credential_shaped(subject)
        return _bound_subject(subject)
    if isinstance(principal, str) and principal:
        return _bound_untyped_subject(principal)
    return _bound_untyped_subject(str(principal))


async def _record_admin_action(
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
    """THE LEAF: the audit row's INSERT itself (the body
    :func:`record_admin_action` routes to — never the module attr, which
    a rebinding can flip mid-chain)."""
    await conn.execute(
        _INSERT_SQL.format(schema=schema),
        principal_subject(principal),
        action,
        target_type,
        target_id,
        reason,
        dumps_jsonb_str(detail if detail is not None else {}),
    )


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

    THE CAPABILITY SEAM (see the module docstring): when the admin shim
    is already loaded, the row resolves through ITS current module
    attribute — the shim's attribute surface is the ONE seam (a rebinding
    there — a test double, a host's instrumentation — bites every writer
    in the process).

    TWO DEFENSES, both born red (the red-team's CHANGES-REQUIRED; the
    teeth pins ``test_wf_attack_cancel.py`` convict each, live):

    * THE FROZEN IDENTITY (``_CANONICAL_RECORD``, bound once AFTER this
      def): the fall-through guard compares the shim's attr against the
      FROZEN function object, never the module global. The global IS
      rebindable (the red-team's 1e — the newcomer's natural patch
      target: a delegating spy on ``taskq.audit.record_admin_action``);
      a global comparison made the body route back to itself — unbounded
      RecursionError poisoning the whole cancel.
    * THE RE-ENTRANCY GUARD (``_routing``, a :class:`contextvars.ContextVar`):
      while THIS call chain is routing through the loaded shim's writer,
      a RE-ENTRY (the red-team's 1c — a delegating wrapper on the shim's
      attr calling the canonical body) lands its row DIRECTLY — no
      re-routing, no loop; the wrapper's call chain terminates with
      EXACTLY ONE row. THE CONTEXTVAR IS THE CHOICE, deliberately: the
      re-entrancy domain is the ASYNC CALL CHAIN — asyncio copies the
      context per Task (and per thread), so two concurrent cancels never
      see each other's routing state (a ``threading.local`` would leak
      the flag ACROSS tasks interleaving on one loop — a mid-await
      handoff would make the second cancel silently BYPASS the shim's
      seam); a sentinel attribute on the wrapper fails on callables that
      accept no attributes (``functools.partial``, builtins) and leaks
      the same way across tasks sharing one wrapper.

    THE MID-CALL WRITER-SWITCH BOUNDARY (the honesty note): the writer
    is resolved ONCE, at this seam's entry. A rebinding of the shim's
    attribute (or the canonical global) BETWEEN the resolution and the
    row's write is NOT observed by this call — the row routes to the
    writer that was current AT RESOLUTION TIME. A mid-call switch takes
    effect on the NEXT call chain; no call lands a row through a writer
    that changed underneath it mid-flight.
    """
    if _routing.get():
        # THE RE-ENTRY: this call chain is already routing through the
        # shim's writer — land the row HERE (the leaf). The delegation
        # chain terminates: exactly one row per call chain.
        await _record_admin_action(
            conn,
            schema=schema,
            principal=principal,
            action=action,
            target_type=target_type,
            target_id=target_id,
            reason=reason,
            detail=detail,
        )
        return
    mod = _loaded_admin_audit()
    if mod is not None:
        writer = getattr(mod, "record_admin_action", None)
        if writer is not None and writer is not _CANONICAL_RECORD:
            token = _routing.set(True)
            try:
                await writer(
                    conn,
                    schema=schema,
                    principal=principal,
                    action=action,
                    target_type=target_type,
                    target_id=target_id,
                    reason=reason,
                    detail=detail,
                )
            finally:
                _routing.reset(token)
            return
    await _record_admin_action(
        conn,
        schema=schema,
        principal=principal,
        action=action,
        target_type=target_type,
        target_id=target_id,
        reason=reason,
        detail=detail,
    )


#: THE FROZEN CANONICAL IDENTITY — bound ONCE, after the def (the
#: red-team's 1e cure): the routing guard compares the shim's attr
#: against THIS object, never the module global (which any rebinding
#: flips). A delegating spy patched over the module global terminates:
#: the body sees the shim's attr still carrying THIS object and falls
#: through to the leaf.
_CANONICAL_RECORD = record_admin_action


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
        _failed_counter().add(1, {"action": action, "operation": "record"})
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
        _failed_counter().add(1, {"action": ACTION_JOB_CANCEL, "operation": "fold"})
        logger.warning(
            "admin-audit-cancel-fold-failed",
            target_type=TARGET_TYPE_JOB,
            target_id=str(job_id),
            error_type=type(exc).__name__,
            error=str(exc),
        )
