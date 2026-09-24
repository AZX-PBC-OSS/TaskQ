"""ATTACK pin: no writer can hard-DELETE a non-terminal job row.

The retention-vs-consumer loss class has a sibling: a writer that deletes a
LIVE row (a cancel-and-delete admin op, a maintenance shortcut) does not
move the row anywhere - it destroys it, silently, for every consumer. The
fleet's contract is that a job row leaves ``jobs`` by exactly two doors:

1. the archive move (``_ARCHIVE_CTE_SQL``'s ``deleted`` arm, the only
   ``DELETE FROM jobs`` in the package): terminal status + aged past
   retention + FOR UPDATE SKIP LOCKED + re-verified still-terminal at the
   delete, and the row is conserved into ``jobs_archive`` by the same
   statement (the conservation pins own that half);
2. nothing else. Cancel is a status transition, the expiry sweep touches
   ``jobs_archive`` only, and every other DELETE in the package targets
   ``job_events`` / side tables.

This pin is the tripwire that keeps the contract proven rather than
remembered: it scans every packaged module for ``DELETE FROM`` statements
and classifies each target table. A new DELETE that can touch a live job
row fails here until it is either classified as one of the known retention
doors (with its guards) or fixed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from taskq.worker import (
    _leader_shared,  # pyright: ignore[reportPrivateUsage]  # Why: the pin binds the exact statement text the sweep machinery renders.
)

_PKG_ROOT = Path(_leader_shared.__file__).parent.parent

#: Tables a packaged DELETE may target without threatening a live job row:
#: side tables and the archive tiers. A live row never sits in any of these.
_SANCTIONED_TABLES: frozenset[str] = frozenset(
    {
        # Retention and bookkeeping side tables.
        "job_events",
        "job_attempts",
        "job_attempts_archive",
        "jobs_archive",
        "rate_limit_buckets",
        "rate_limit_window_entries",
        "reservation_slots",
        "cron_schedules",
        "batches",
        "admin_audit",
        "saml_replay_store",
        "actor_config",
        "queues",
        "workers",
        "maintenance_leader",
    }
)

_DELETE_RE = re.compile(r"DELETE\s+FROM\s+\"?\{?(schema|s)?\}?\"?\.?\"?(\w+)\"?", re.IGNORECASE)


def _packaged_sql_delete_targets() -> dict[str, set[str]]:
    """file -> the set of DELETE target tables its SQL statements mention.

    Source scan with docstrings removed by line span, not an SQL parse:
    the statements are package-owned (no user input reaches a table name,
    every dynamic identifier is validated against _IDENT_RE), so the
    literal text is the truth the reviewer sees and the tripwire fires the
    moment a new statement is written. Docstrings are excluded by their
    AST line spans, which is what keeps prose like "the archive move's
    ``DELETE FROM jobs`` cascade" from polluting the classification while
    a real SQL literal - f-string-split pieces included - counts.
    """
    import ast

    targets: dict[str, set[str]] = {}
    for path in sorted(_PKG_ROOT.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        docstring_lines: set[int] = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                body = node.body
                first = body[0] if body else None
                if (
                    first is not None
                    and first.end_lineno is not None
                    and isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                ):
                    docstring_lines.update(range(first.lineno, first.end_lineno + 1))
        stripped = "\n".join(
            line
            for no, line in enumerate(source.splitlines(), start=1)
            if no not in docstring_lines and not line.lstrip().startswith("#")
        )
        found = {m.group(2) for m in _DELETE_RE.finditer(stripped)}
        if found:
            targets[str(path.relative_to(_PKG_ROOT))] = found
    return targets


class TestNoLiveJobDelete:
    def test_the_only_jobs_delete_is_the_archive_move(self) -> None:
        """Exactly one packaged module may name a ``jobs`` delete target,
        and it is the archive CTE. Any second site is an unclassified
        writer and fails here."""
        targets = _packaged_sql_delete_targets()
        assert targets, "the scan found nothing: the pin rotted"
        jobs_sites = {file: tables for file, tables in targets.items() if "jobs" in tables}
        assert jobs_sites == {"worker/_leader_shared.py": {"jobs", "jobs_archive", "workers"}}, (
            f"an unclassified DELETE can reach the jobs table: {jobs_sites} - "
            "classify it in this pin with the guard that makes it "
            "retention-only, or fix it: a writer that can delete a live job "
            "row is a silent drop, the worst defect class this repo has"
        )

    def test_the_archive_moves_guards_are_in_the_statement_text(self) -> None:
        """The sanctioned door keeps its guards IN the statement: the
        terminal-status re-check at lock time, the still-terminal re-check
        at delete time, the SKIP LOCKED that refuses a row a retry holds,
        and the age bound that keeps the window a retention window.
        Removing any guard is a live-row drop and fails here."""
        sql: str = _leader_shared._ARCHIVE_CTE_SQL  # pyright: ignore[reportAttributeAccessUsage]
        deleted_arm = sql.split('"deleted AS"')[-1]
        assert "DELETE FROM" in deleted_arm
        assert "status = $1" in deleted_arm, (
            "the delete arm's still-terminal re-check is gone: a version "
            "change between archive and delete would destroy a live row"
        )
        locked_arm = sql.split("locked AS MATERIALIZED")[1].split("moved AS")[0]
        assert "SKIP LOCKED" in locked_arm, (
            "the lock must refuse a row another transaction holds (a retry "
            "in flight): without it the prune deletes a live row"
        )
        assert "j.status = $1" in locked_arm, (
            "the lock-time terminal re-check is gone: a non-terminal row "
            "must never enter the delete set"
        )
        assert "finished_at < statement_timestamp()" in locked_arm, (
            "the age bound is gone: the delete would fire on fresh terminal "
            "rows, a retention window that reads as zero"
        )
        # The move precedes the delete in the same statement: the row is
        # conserved before it is removed, the archive-once guard keeping
        # the INSERT from colliding with a standing archive row.
        assert sql.index("INSERT INTO") < sql.index("DELETE FROM"), (
            "the conservation INSERT must precede the delete in statement "
            "order: the delete is the move's second half, never a standalone"
        )
        assert "NOT EXISTS (SELECT 1 FROM" in sql, (
            "the archive-once guard is gone: a fold must converge, not wedge"
        )

    @pytest.mark.parametrize(
        "table",
        sorted(
            _SANCTIONED_TABLES
            - {
                "jobs",
                "jobs_archive",
                "workers",
                "job_attempts",
                "job_attempts_archive",
                "admin_audit",
            }
        ),
    )
    def test_sanctioned_tables_are_actually_deleted_somewhere(self, table: str) -> None:
        """Guard against the sanctioned set rotting into fiction: every
        directly-deleted table the pin blesses must actually be deleted by
        the package, so removing a real deleter or adding a fake entry
        shows up here. (job_attempts_archive is cascade-deleted with
        jobs_archive and admin_audit is append-only by design, neither has
        a direct statement; that is the sanctioned set's documented shape,
        not drift.)"""
        targets = _packaged_sql_delete_targets()
        assert any(table in tables for tables in targets.values()), (
            f"{table} is sanctioned in this pin but no packaged statement "
            "deletes it: the sanctioned set has drifted from the package"
        )
