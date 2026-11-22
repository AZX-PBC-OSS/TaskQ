"""The crash lane's own set/census claims must be backed by its mechanism.

``tests/system_e2e/test_wf_identity_boundary_kill.py`` asserts more than its
mechanism delivers. Three of its legs have claimed more than their mechanism:

* the fenced-phantom set is equated to *every* ``running`` record claim the
  corpse held. A record row is claimed ``running`` by the worker's producer
  (``taskq.worker.run`` / the ``api/_runner`` claim before
  ``claim_step_ledger``) before the workflow ledger has a row for it, and
  ``PHANTOM_REAP_SQL`` fences *ledger* rows only. A claimed-but-unledgered
  record therefore has no phantom to fence, so the equality can false-red
  correct behaviour (measured: ``corpse_records=2 fenced_ids=1``);
* the census is tagged once, right after ``create_flow``, while the records
  are emitted later. ``assert_balanced`` then runs over the two pre-emit rows
  only (measured: ``tagged_before_emit=2 total_rows=7 tagged_after_emit=2``),
  so the emitted population is outside the conservation invariants;
* the retry leg equated the re-executed set (``attempt > 1``) with the
  corpse's in-flight claims. The attempt counter increments at claim
  (``backend/_dispatch_sql.py``: ``attempt = LEAST(j.attempt + 1, 32767)``),
  and the machinery's own at-least-once reclaim (a lapsed lease handed back
  mid-flight, a heartbeat-lost isolation, a repend with backoff) re-executes
  rows the corpse never held — so the equality false-reds correct behaviour
  the first time the set legs run with ``n > 0`` (measured: ``retried (3) !=
  corpse_records (2)``, zero lost, one terminal per identity).

The defects are in the lane's assertions, not in the product. There is no
behavioural input that can turn the lane red for them (a green run has never
seen a non-zero corpse set), so these pins hold the *lane's contract* to its
mechanism: they read the lane's own AST and fail while the unsound shape is
present. They are the executable statement of what the repair must deliver.

The checks identify the sets by the QUERY they come from, not by their
variable names, so a repair that renames ``corpse_records`` or ``fenced_ids``
still passes as long as the sets' provenance is the same.

WHY EACH PIN CAN FAIL (and what makes it pass):

* ``test_fence_set_is_the_corpse_ledger_claim_subset`` fails on the shipped
  ``assert fenced_ids == corpse_records`` and passes once the fence comparison
  targets the corpse's ``wf_step_ledger``-claimed subset while the unledgered
  claims are allowed to have no phantom;
* ``test_census_is_restamped_immediately_before_assert_balanced`` fails
  because the only ``tag_run_rows`` call precedes the emit and no assertion
  reads the tagged population; it passes once the lane re-stamps after the
  run settles, asserts the tagged census covers every run row, and only then
  calls ``assert_balanced``;
* ``test_lane_forces_a_nonzero_corpse_claim_set`` fails because the lane never
  waits for a record claim before the SIGKILL and never asserts the snapshot
  is non-empty; it passes once a pre-kill gate forces ``n > 0`` and the lane
  asserts it, so both set legs are exercised;
* ``test_retry_leg_does_not_equate_attempt_gt_1_to_the_corpse_set`` fails on
  ``assert retried == corpse_records`` and passes once the retry leg demands
  only the zero-loss direction (``set(corpse_records) - retried`` empty) and
  bounds the machinery's extras instead of equating the two sets. It names the
  instructed equality shape; a set-equality written in another form is not
  detected by it.
"""

from __future__ import annotations

import ast
from itertools import pairwise
from pathlib import Path

import pytest

_LANE = Path(__file__).with_name("system_e2e") / "test_wf_identity_boundary_kill.py"

#: The maximum gap between the census stamp and ``assert_balanced``. The
#: harness requires the stamp "immediately before" the invariant read; this
#: window leaves room for the coverage assertion between them.
_CENSUS_WINDOW_LINES = 30


def _lane_source() -> tuple[str, ast.Module]:
    source = _LANE.read_text(encoding="utf-8")
    return source, ast.parse(source, filename=str(_LANE))


def _segment(source: str, node: ast.AST) -> str:
    return ast.get_source_segment(source, node) or ""


def _names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _assigned_names(node: ast.Assign | ast.AnnAssign) -> set[str]:
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    names: set[str] = set()
    for target in targets:
        for item in ast.walk(target):
            if isinstance(item, ast.Name):
                names.add(item.id)
    return names


def _assigned_names_matching(source: str, tree: ast.Module, *tokens: str) -> set[str]:
    """Names assigned from an expression whose source contains every *token*."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            segment = _segment(source, node)
            if all(token in segment for token in tokens):
                names |= _assigned_names(node)
    return names


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _asserts(tree: ast.Module) -> list[ast.Assert]:
    return [node for node in ast.walk(tree) if isinstance(node, ast.Assert)]


def _predicate_segment(source: str, tree: ast.Module, call: ast.Call) -> str:
    """The wait's predicate source, following a named predicate to its def."""
    if not call.args:
        return ""
    predicate = call.args[0]
    segment = _segment(source, predicate)
    if isinstance(predicate, ast.Name):
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == predicate.id
            ):
                segment += "\n" + _segment(source, node)
    return segment


def _eq_sides(compare: ast.Compare) -> list[tuple[ast.expr, ast.expr]]:
    """Every adjacent operand pair of a chained comparison."""
    operands = [compare.left, *compare.comparators]
    return list(pairwise(operands))


def _is_eq(compare: ast.Compare) -> bool:
    return any(isinstance(op, ast.Eq) for op in compare.ops)


def _corpse_set_names(source: str, tree: ast.Module) -> set[str]:
    """The snapshot of the victim's running record rows, by query provenance."""
    return _assigned_names_matching(
        source,
        tree,
        "SELECT id",
        "locked_by_worker",
        "status = 'running'",
    )


def _fenced_set_names(source: str, tree: ast.Module) -> set[str]:
    """The fenced ``wf_step_ledger`` job ids, by query provenance."""
    return _assigned_names_matching(
        source,
        tree,
        "SELECT job_id",
        "wf_step_ledger",
        "status = 'fenced'",
    )


def _retried_set_names(source: str, tree: ast.Module) -> set[str]:
    """The re-executed record set (``attempt > 1``), by query provenance."""
    return _assigned_names_matching(source, tree, "SELECT id", "attempt > 1")


def test_fence_set_is_the_corpse_ledger_claim_subset() -> None:
    source, tree = _lane_source()
    corpse_names = _corpse_set_names(source, tree)
    assert corpse_names, (
        "the lane no longer snapshots the corpse's running record claims "
        "(a query on locked_by_worker + status = 'running' + BOUNDARY_STEP)"
    )
    fenced_names = _fenced_set_names(source, tree)
    assert fenced_names, (
        "the lane no longer reads the fenced wf_step_ledger job ids "
        "(SELECT job_id ... status = 'fenced')"
    )

    # The unsound equality: the fenced ledger rows are the corpse's *record
    # claims*, but a claimed-but-unledgered claim has no ledger row to fence.
    for assertion in _asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare) or not _is_eq(test):
            continue
        for left, right in _eq_sides(test):
            if _names(left) & fenced_names and _names(right) & corpse_names:
                pytest.fail(
                    "the lane equates the fenced phantom set to EVERY corpse "
                    "record claim. A row claimed running before the ledger "
                    "claim has no wf_step_ledger row for PHANTOM_REAP_SQL to "
                    "fence, so this false-reds correct behaviour: "
                    f"{_segment(source, assertion).splitlines()[0].strip()}"
                )
            if _names(left) & corpse_names and _names(right) & fenced_names:
                pytest.fail(
                    "the lane equates the corpse's every record claim to the "
                    "fenced phantom set; claimed-but-unledgered records have "
                    "no phantom: "
                    f"{_segment(source, assertion).splitlines()[0].strip()}"
                )

    # The sound target: a subset derived from the corpse's own ledger rows,
    # either assigned and then compared, or selected inline in the assert.
    ledger_names = _assigned_names_matching(source, tree, "wf_step_ledger", *corpse_names)
    for assertion in _asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare) or not _is_eq(test):
            continue
        for left, right in _eq_sides(test):
            for fence_side, other in ((left, right), (right, left)):
                if not (_names(fence_side) & fenced_names):
                    continue
                other_segment = _segment(source, other)
                if _names(other) & ledger_names or (
                    "wf_step_ledger" in other_segment
                    and (
                        _names(other) & corpse_names
                        or any(name in other_segment for name in corpse_names)
                    )
                ):
                    return

    pytest.fail(
        "the lane has no fence assertion against the corpse's wf_step_ledger "
        "claim subset; fenced ids must be compared to the ledger rows the "
        "corpse actually held, not to every running record claim"
    )


def test_census_is_restamped_immediately_before_assert_balanced() -> None:
    source, tree = _lane_source()
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    balanced = [call for call in calls if _call_name(call) == "assert_balanced"]
    assert len(balanced) == 1, (
        f"expected exactly one assert_balanced call in the lane, found {len(balanced)}"
    )
    balanced_line = balanced[0].lineno

    terminal_waits = [call for call in calls if _call_name(call) == "_wait_terminal"]
    assert terminal_waits, "the lane no longer calls _wait_terminal; this pin must be re-read"

    stamps = [call for call in calls if _call_name(call) == "tag_run_rows"]
    before = [call for call in stamps if call.lineno < balanced_line]
    assert before, (
        "the lane never calls tag_run_rows before assert_balanced, so the "
        "invariant population is whatever an earlier stamp happened to catch"
    )
    nearest = max(before, key=lambda call: call.lineno)
    assert nearest.lineno > max(call.lineno for call in terminal_waits), (
        "the census stamp precedes the run's terminal wait; records emitted "
        "after the stamp stay outside the invariant population"
    )
    assert balanced_line - nearest.lineno <= _CENSUS_WINDOW_LINES, (
        "the census stamp is not immediately before assert_balanced: the "
        f"stamp is at line {nearest.lineno}, the invariant read at {balanced_line} "
        f"(window {_CENSUS_WINDOW_LINES})"
    )

    # The stamp must be accompanied by a read that proves the population
    # actually covers the run — a row-count assertion, not an assumption.
    tagged_targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and "tags @>" in _segment(source, node):
            tagged_targets |= _assigned_names(node)

    for assertion in _asserts(tree):
        if _names(assertion.test) & tagged_targets:
            return
        if "tags @>" in _segment(source, assertion):
            return

    pytest.fail(
        "the lane re-stamps the census but never asserts that the tagged "
        "population covers the run's rows; a stamp without a coverage "
        "assertion discharges nothing"
    )


def test_lane_forces_a_nonzero_corpse_claim_set() -> None:
    source, tree = _lane_source()
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    corpse_names = _corpse_set_names(source, tree)
    assert corpse_names, "the lane no longer snapshots the corpse's record claims"

    sigkill_calls = [
        call
        for call in calls
        if _call_name(call) == "send_signal" and "SIGKILL" in _segment(source, call)
    ]
    assert sigkill_calls, "the lane no longer SIGKILLs the holder; this pin must be re-read"
    sigkill_line = min(call.lineno for call in sigkill_calls)

    # A test-side gate must make the victim hold a record when it dies. The
    # gate is a wait before the signal, reading the record claim by worker.
    def _has_claim_tokens(segment: str) -> bool:
        return "locked_by_worker" in segment and "BOUNDARY_STEP" in segment

    def _is_claim_gate(node: ast.AST) -> bool:
        if isinstance(node, ast.Call) and _call_name(node) == "_wait_for":
            return _has_claim_tokens(_predicate_segment(source, tree, node))
        if isinstance(node, (ast.While, ast.For, ast.AsyncFor)):
            return _has_claim_tokens(_segment(source, node))
        return False

    gates = [
        node
        for node in ast.walk(tree)
        if getattr(node, "lineno", sigkill_line) < sigkill_line and _is_claim_gate(node)
    ]
    assert gates, (
        "no test-side gate waits for the victim to hold a record claim before "
        "the SIGKILL; every measured green run killed a corpse with zero "
        "in-flight record claims, so the set legs never ran"
    )

    # And the lane must assert the snapshot is non-empty, not just print it.
    for assertion in _asserts(tree):
        test = assertion.test
        segment = _segment(source, assertion)
        if not (_names(test) & corpse_names or any(name in segment for name in corpse_names)):
            continue
        if isinstance(test, ast.Name) and test.id in corpse_names:
            return
        for node in ast.walk(test):
            if (
                isinstance(node, ast.Compare)
                and _names(node) & corpse_names
                and any(isinstance(op, (ast.Gt, ast.GtE, ast.NotEq)) for op in node.ops)
            ):
                return

    pytest.fail(
        "the lane snapshots the corpse's record claims but never asserts the "
        "set is non-empty; a green that only ever sees n=0 does not exercise "
        "the retry or fence legs"
    )


def test_retry_leg_does_not_equate_attempt_gt_1_to_the_corpse_set() -> None:
    source, tree = _lane_source()
    corpse_names = _corpse_set_names(source, tree)
    assert corpse_names, "the lane no longer snapshots the corpse's record claims"
    retried_names = _retried_set_names(source, tree)
    assert retried_names, (
        "the lane no longer derives the re-executed record set from its own "
        "attempt > 1 query; this pin must be re-read"
    )

    for assertion in _asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare):
            continue
        operands = [test.left, *test.comparators]
        for left, op, right in zip(operands[:-1], test.ops, operands[1:], strict=True):
            left_retried = bool(_names(left) & retried_names)
            right_retried = bool(_names(right) & retried_names)
            left_corpse = bool(_names(left) & corpse_names)
            right_corpse = bool(_names(right) & corpse_names)
            over_claims = (
                (
                    isinstance(op, ast.Eq)
                    and ((left_retried and right_corpse) or (left_corpse and right_retried))
                )
                or (isinstance(op, ast.LtE) and left_retried and right_corpse)
                or (isinstance(op, ast.GtE) and left_corpse and right_retried)
            )
            if not over_claims:
                continue
            pytest.fail(
                "the lane demands that the re-executed record set (attempt > 1) "
                "be exactly the corpse's in-flight claims. The reclaim "
                "machinery's own at-least-once path (a lapsed lease handed "
                "back mid-flight, a heartbeat-lost isolation, a repend) "
                "re-executes rows the corpse never held — the attempt counter "
                "increments at claim — so this false-reds correct behaviour "
                "(measured: retried (3) != corpse_records (2), zero lost). Keep "
                "the zero-loss direction (corpse claims ⊆ retried) and bound "
                "the machinery's extras: "
                f"{_segment(source, assertion).splitlines()[0].strip()}"
            )
