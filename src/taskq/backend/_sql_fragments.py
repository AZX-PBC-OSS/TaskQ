"""Shared SQL constants and statement fragments, importable everywhere.

This module carries the single-source pieces of the shipped SQL that more
than one module reads: the deadline-failure message texts, the terminal-write
fence predicate, the attempt-refund expression, and the pre-rendered
non-consuming deferral floor. The pre-rendered statement bundle
(``taskq.backend._sql_templates``) interpolates them; the in-memory twins in
``taskq.testing`` read the same constants so the two backends cannot drift.

The module is deliberately import-only: no backend implementation, no
driver, no runtime dependencies beyond ``taskq.constants``. The testing
package's driver-free surface (pinned by
``tests/test_memory_jobs_fixture.py::test_testing_no_transitive_asyncpg``)
imports these constants at module level, so nothing here may reach
``asyncpg`` or any other database package, even transitively.
"""

from typing import Final

from taskq.constants import MIN_DEFERRAL_INTERVAL

__all__ = [
    "DEADLINE_EXCEEDED_MESSAGE",
    "DEADLINE_RETRY_EXCEEDED_MESSAGE",
]

# The non-consuming deferral floor, pre-rendered for the two arms that
# carry it (mark_snoozed's snoozed arm and mark_retry_after's
# consume_budget=False snoozed arm). Derived from the constant so the
# SQL and the in-memory twin read one value and cannot drift.
_MIN_DEFERRAL_INTERVAL_SQL: Final[str] = (
    f"interval '{MIN_DEFERRAL_INTERVAL.total_seconds()} seconds'"
)

# The non-consuming release's attempt refund, ONE fragment shared by every
# release arm that hands back an attempt the actor did not finish on its
# own terms AND did not start: mark_snoozed's 'snoozed'/'unavailable' arm,
# mark_retry_after_consume_false's snoozed arm, and the shutdown drain's
# hand-back (worker/shutdown.py imports this constant). Dispatch stamps
# ``attempt = j.attempt + 1`` at claim (backend/_dispatch_sql.py); a
# release that executed nothing returns the increment, floored at 0. The
# refund revisits an attempt number, which is collision-safe: a
# non-terminal release writes no job_attempts row, so the PK (job_id,
# attempt) is never revisited by a writer (see the mark_snoozed comment
# block in backend/_sql_templates.py for the full argument).
#
# mark_interrupted's release arm is deliberately NOT a consumer: its
# attempt DID start executing, so refunding the increment re-creates the
# exact attempt epoch the interrupted (zombie) handler still holds; the
# zombie's later terminal write then passes the attempt fence and lands
# on the re-dispatched attempt, and the live execution's own terminal
# write no-ops. An interruption charges the attempt it ran.
# The reference is alias-qualified (``j.``): every consumer of the
# fragment aliases its target table ``j``.
#
# None of these release arms wakes the fleet. A row they land as
# 'pending' is re-pended by UPDATE, which the INSERT-only wake trigger
# never sees, and no arm issues a pg_notify of its own: the producer's
# poll floor (WorkerSettings.notify_poll_interval with NOTIFY on,
# poll_interval without) is the wake source for a released row, the same
# trade the sweeps' UPDATE re-pends would make were they not batched
# behind their own single notify. A release is the worker giving a row
# back (a snooze, a retry-after, a shutdown interrupt), never new work,
# so a claim within the poll floor is the intended latency.
_ATTEMPT_REFUND_SQL: Final[str] = "GREATEST(j.attempt - 1, 0)"

# The deadline-failure message and its mark_retry variant, ONE constant per
# spelling (the _ATTEMPT_MESSAGES pattern): every schedule_to_close terminal
# exit stamps the message on the row, the job_attempts row, and the
# state_change event, and the same texts are pinned verbatim by the
# differential corpus, so the templates and the in-memory twins read the two
# constants instead of hand-restating the strings. The variant exists because
# mark_retry's deadline arm names the deadline that actually fires: the
# schedule_to_close check there includes the retry delay itself, so the next
# dispatch the deadline cuts off is the NEXT RETRY dispatch, not the next
# ordinary one. NO value may change: the pins assert exact texts.
DEADLINE_EXCEEDED_MESSAGE: Final[str] = "schedule_to_close reached before next dispatch"
DEADLINE_RETRY_EXCEEDED_MESSAGE: Final[str] = "schedule_to_close reached before next retry dispatch"

# The terminal-write fence, ONE fragment per spelling (the {has_budget}
# mechanism): every worker-scoped terminal or deferral arm re-checks that the
# row is still running, still locked by THIS worker, and still at the
# attempt epoch the caller presents, and a fence conjunct edited in one
# statement but not its siblings is exactly the drift that lets a stale
# handler's write land (see the attempt-epoch fencing note in
# backend/_sql_templates.py's render()). The aliased spelling is the multi-arm
# arbiters' (they resolve ids through the params CTE); the bound spelling is
# the single-row mark_succeeded / mark_failed UPDATEs, which bind $2/$8
# directly. Substituted by name into the templates so the rendered statements
# stay byte-identical to the hand-maintained conjuncts; the in-memory twin
# mirrors the predicate through its own _fenced helper (testing/_terminal.py),
# the same one-predicate discipline on the Python side.
_JOB_FENCE_SQL: Final[str] = (
    "AND j.status = 'running'\n"
    "      AND j.locked_by_worker = (SELECT worker_id FROM params)\n"
    "      AND j.attempt = (SELECT attempt FROM params)"
)
_JOB_FENCE_BOUND_SQL: Final[str] = (
    "id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $8"
)
