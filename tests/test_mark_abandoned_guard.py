"""The abandon guard's fencing predicates, pinned at the SQL-template level.

Why a template pin: the abandon path's terminal-stays-terminal protection is
carried entirely by predicates inside the ``_sql_templates.mark_abandoned``
string literal. Nothing in the Python source changes when one is dropped, so
the predicates execute (and count as covered) with nothing asserting them;
``tests/test_backend_fencing_invariants.py`` observes the guard's outcomes on
live rows but only in the integration lane, and the cross-worker pin
(``tests/test_rt_cancelwatch_cross_worker_abandon.py``) reaches the guard on
exactly one row shape: the terminalised cancel-phase row PR #272's reclaim
leaves behind. This pin keeps the guard's full membership observable on every
shape, in the fast lane, pinned against the same constant the backend renders
(a copy would drift from the SQL that actually runs).

What is pinned and why:

* ``status = 'running'`` -- the fence that makes a terminal row stay
  terminal. ``mark_abandoned`` deliberately leaves ``locked_by_worker`` and
  ``lock_expires_at`` in place (they are the audit trail of which worker was
  abandoned), so a zombie actor's stale abandon can still name the row; only
  this clause stops it from writing a second terminal state over an
  already-terminal row (a cancelled, crashed, failed or previously-abandoned
  one alike).
* ``cancel_phase = 2`` -- abandon applies only once the cooperative protocol
  has escalated to phase 2. Below that the row's live holder still owns the
  outcome, and the escalation ladder (not the abandon) must move the row.
* ``lock_expires_at IS NULL`` -- the template's defense-in-depth arm for the
  no-exit cell (running x NULL lease x dead holder); the template's own
  comment documents why that arm abandons directly.

What is deliberately NOT pinned: the absence of a worker fence. The statement
probes the row's OWN ``locked_by_worker`` (the ``holder`` CTE reads it back
from ``upd``), so the caller-side cancel-poll predicate
(``locked_by_worker = $1 AND cancel_requested_at IS NOT NULL AND
status = 'running'``) is the worker fence; a future defense-in-depth worker
id guard added to this statement is a welcome tightening, not a red test.
"""

import re

from taskq.backend._sql_templates import render

# The upd CTE's guard: from its keyed WHERE to the RETURNING that closes it.
_GUARD_RE = re.compile(r"WHERE id = \$1(?P<guard>.*?)RETURNING", re.DOTALL)


def test_mark_abandoned_guard_fences_on_running_and_escalation() -> None:
    sql = render("mark_abandoned_guard_probe").mark_abandoned
    match = _GUARD_RE.search(sql)
    assert match is not None, (
        "the upd CTE's keyed WHERE guard moved or vanished; re-anchor this "
        "pin on the new guard and re-derive which fences it still carries"
    )
    guard = match.group("guard")

    assert "status = 'running'" in guard, (
        "mark_abandoned's guard dropped the running fence: the statement can "
        "now write a second terminal state over an already-terminal row, and "
        "the abandon's audit columns (locked_by_worker, lock_expires_at kept "
        "in place) mean the zombie holder's stale abandon reaches it. This "
        "fence is what makes a terminalised cancel-phase row (the PR #272 "
        "reclaim's output) immune to a stale abandon."
    )
    assert "cancel_phase = 2" in guard, (
        "mark_abandoned's guard dropped the escalation requirement: the "
        "abandon would apply to a row whose cooperative protocol has not "
        "reached phase 2, ending a cancel the live holder still owns"
    )
    assert "lock_expires_at IS NULL" in guard, (
        "mark_abandoned's guard dropped the NULL-lease defense-in-depth arm "
        "for the no-exit cell (running x NULL lease x dead holder); the "
        "template's own comment documents why that arm abandons directly"
    )
