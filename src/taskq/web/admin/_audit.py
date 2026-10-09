"""Admin audit trail: the admin package's COMPAT SHIM over the writer.

The canonical writer is :mod:`taskq.audit` — the DEPS-FREE home (the
import law, §16.1: the workflow engine's cancel/retry seams import the
writer from THERE, because THIS package's ``__init__`` imports the
``fastapi`` extra — a base ``taskq[flows]`` install must be able to
cancel a run without ever loading this package; the attack pins' Face C
walks ``taskq/workflows/`` for exactly that import and reds on it).

Every name here is the canonical object re-exported: the admin routes
and the tests keep their ``taskq.web.admin._audit`` import path, and the
CAPABILITY SEAM (``taskq.audit.record_admin_action``'s resolution) routes
through THIS module's CURRENT attribute whenever the shim is loaded — a
rebinding here (a test double, a host's instrumentation) is the one seam
that bites every audit writer in the process. See :mod:`taskq.audit`.
"""

from taskq.audit import (
    ACTION_ACTOR_DEREGISTER,
    ACTION_JOB_CANCEL,
    ACTION_JOB_RETRY,
    ACTION_RATE_LIMIT_RESET,
    ACTION_SCHEDULE_DISABLE,
    ACTION_SCHEDULE_ENABLE,
    ACTION_SCHEDULE_RUN,
    ACTION_SCHEDULE_SKIP,
    ANONYMOUS_SUBJECT,
    SUBJECT_MAX_LENGTH,
    TARGET_TYPE_ACTOR,
    TARGET_TYPE_JOB,
    TARGET_TYPE_RATE_LIMIT_BUCKET,
    TARGET_TYPE_SCHEDULE,
    fold_principal_into_cancel_event,
    logger,
    principal_subject,
    record_admin_action,
    record_admin_action_safe,
)
from taskq.audit import (
    _record_failed_counter as _record_failed_counter,  # pyright: ignore[reportPrivateUsage]  # Why: the private degradation counter's COMPAT RE-EXPORT (the explicit-reexport form: the name exists to be REBOUND by the admin redteams' monkeypatches — the capability seam resolves the counter through THIS module's attribute when loaded; see taskq.audit._failed_counter) — never imported by runtime code.
)

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
    "logger",
    "principal_subject",
    "record_admin_action",
    "record_admin_action_safe",
]
